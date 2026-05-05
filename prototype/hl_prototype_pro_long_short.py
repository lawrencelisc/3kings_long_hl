"""
================================================================================
  hl_dualside_v1.py
  Base:     hl_prototype_pro_long.py  (Hyperliquid Long V1)
  Upgrade:  Added full SHORT side — same exchange, same capital pool
  Strategy: Dual-Direction Momentum Sniper (Long + Short)
  Exchange: Hyperliquid (via CCXT) | Base currency: USDC
================================================================================

  ARCHITECTURE — HOW LONG & SHORT COEXIST
  ─────────────────────────────────────────
  The BTC regime navigator now returns a 3-way signal:
    +1  → Bullish  : scout LONGS  (HMA20 > HMA50, ADX confirmed)
     0  → Neutral  : no new entries, manage existing positions only
    -1  → Bearish  : scout SHORTS (HMA20 < HMA50, ADX confirmed)

  Position namespace:
    long_positions  dict  — keyed by symbol, holds long  trades
    short_positions dict  — keyed by symbol, holds short trades
  Both share the same cooldown_tracker and consecutive_losses dicts
  (keyed as "SYMBOL_long" / "SYMBOL_short" to avoid cross-contamination).

  Capital allocation:
    Each side draws from WORKING_CAPITAL independently via get_live_usdc_balance().
    Maximum simultaneous exposure = 2 × WORKING_CAPITAL × MAX_LEVERAGE, but in
    practice the regime filter ensures only ONE side is active at a time.

  SHORT-SPECIFIC LOGIC DIFFERENCES (vs Long):
  ─────────────────────────────────────────────
  [S-1]  Entry: IOC market SELL (open short) at top 3 BID levels
  [S-2]  TP below entry price: entry - TP_ATR_MULT_SHORT * ATR
  [S-3]  SL above entry price: entry + SL_ATR_MULT_SHORT * ATR
  [S-4]  PnL = (entry_price - exit_price) * amount
  [S-5]  Breakeven: SL moves DOWN to entry * 0.998 (locks 0.2% profit)
  [S-6]  Trail SL moves DOWN as price falls (only allowed to fall, never rise)
  [S-7]  Flow health: exits on Z-score > +3.0 (extreme BUY pressure = squeeze)
  [S-8]  Deceleration: accel_z > +2.5 AND recent_flow > 0 AND bids dominate
  [S-9]  Lee-Ready entry: net_flow < 0, accel < 0, imbalance < -0.15 (asks heavy)
  [S-10] Anti-squeeze filter: cancel short if imbalance > +0.1 (bids dominating)
  [S-11] Scouting: weakest coins by 24h % change (ascending = most negative)
  [S-12] Regime: HMA20 < HMA50 (bearish crossover)
  [S-13] Native exit PnL: uses 'buy' fills from fetch_my_trades

  SHARED MODULES (unchanged from Long version):
  ───────────────────────────────────────────────
  • Exchange init, credentials, file paths
  • CSV / status logging
  • Dynamic ban system (JSON-persistent)
  • ATR / market metrics
  • cancel_all_hl, get_3_layer_avg_price
  • get_live_usdc_balance
  • BTC regime navigator (extended to return -1 for bearish)
================================================================================
"""

import ccxt
import pandas as pd
import time
import numpy as np
import os
import logging
import sys
import json
from datetime import datetime

# ==========================================
# ⚙️ [SYSTEM] Logger & Exchange
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('HL_DualSide_V1.0')

API_KEY    = "0x45d7ab3F1cC4B43779b7d931e2D8150E672E2C6b"
API_SECRET = "0x47404a8300c283b911cb9de459785aef3608f62d2d18125d656605dd9e003ea9"

exchange = ccxt.hyperliquid({
    'walletAddress': API_KEY,
    'privateKey':    API_SECRET,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap',
        'user': API_KEY,
    }
})
exchange.load_markets()

# ==========================================
# ⚙️ [FILE PATHS]
# ==========================================
LOG_DIR        = "result"
STATUS_DIR     = "status"
# Separate log files per side for clean post-trade analysis
LOG_FILE_LONG  = f"{LOG_DIR}/live_long_log.csv"
LOG_FILE_SHORT = f"{LOG_DIR}/live_short_log.csv"
STATUS_FILE    = f"{STATUS_DIR}/btc_regime_dual.csv"
BLACKLIST_FILE = f"{STATUS_DIR}/dynamic_blacklist_dual.json"

for d in [LOG_DIR, STATUS_DIR]:
    if not os.path.exists(d):
        os.makedirs(d)

# ==========================================
# ⚙️ [IN-MEMORY STATE]
# Separate position dicts so Long and Short
# cannot accidentally reference each other.
# ==========================================
long_positions  = {}   # symbol → long  position dict
short_positions = {}   # symbol → short position dict
cooldown_tracker   = {}   # "SYMBOL_long" or "SYMBOL_short" → expiry ts
consecutive_losses = {}   # same key scheme

# ==========================================
# ⚙️ [SHARED PARAMETERS]
# ==========================================
WORKING_CAPITAL        = 150.0   # Per-side capital ceiling (USDC)
MAX_LEVERAGE           = 10.0
MIN_NOTIONAL           = 11.0    # HL minimum ~$10
MAX_NOTIONAL_PER_TRADE = 200.0

# --- Signal ---
NET_FLOW_SIGMA       = 1.2

# --- Long params ---
TP_ATR_MULT_LONG     = 3.5   # TP above entry
SL_ATR_MULT_LONG     = 1.0   # SL below entry

# --- Short params ---
# [S-2/S-3] Shorts in a downtrend can run further; wider TP, tighter SL
TP_ATR_MULT_SHORT    = 4.0   # TP below entry (more room for downmove)
SL_ATR_MULT_SHORT    = 0.9   # SL above entry (tight — bearish momentum is fragile)

# --- Ban system ---
MAX_CONSECUTIVE_LOSSES = 3
DYNAMIC_BAN_DURATION   = 86400   # 24 hours

# --- Timing ---
SCOUTING_INTERVAL      = 125
POSITION_CHECK_INTERVAL = 2      # 2s — HL low latency

# --- Risk (same for both sides) ---
RISK_PER_TRADE = 0.01   # 1% of effective capital per trade

# ==========================================
# ⚙️ [BLACKLIST — :USDC symbols]
# ==========================================
BLACKLIST = [
    'USDC/USDC:USDC', 'DAI/USDC:USDC',   'FDUSD/USDC:USDC', 'BUSD/USDC:USDC',
    'TUSD/USDC:USDC', 'PYUSD/USDC:USDC', 'USDP/USDC:USDC',  'EURS/USDC:USDC',
    'USDE/USDC:USDC', 'USAT/USDC:USDC',  'USD0/USDC:USDC',  'USTC/USDC:USDC',
    'LUSD/USDC:USDC', 'FRAX/USDC:USDC',  'MIM/USDC:USDC',   'RLUSD/USDC:USDC',
    'WBTC/USDC:USDC', 'WETH/USDC:USDC',  'WBNB/USDC:USDC',  'WAVAX/USDC:USDC',
    'stETH/USDC:USDC','cbETH/USDC:USDC', 'WHT/USDC:USDC',
]

# ==========================================
# ⚙️ [CSV SCHEMAS]
# ==========================================
CSV_COLUMNS = [
    'timestamp', 'symbol', 'action', 'price', 'amount', 'trade_value',
    'atr', 'net_flow', 'tp_price', 'sl_price', 'reason',
    'realized_pnl', 'actual_balance', 'effective_balance'
]
STATUS_COLUMNS = [
    'timestamp', 'btc_price', 'target_price', 'hma20', 'hma50',
    'adx', 'signal_code', 'decision_text'
]


# ==========================================
# 🛠️ [MODULE 1] CSV Logging
# ==========================================

def log_to_csv(data_dict: dict, side: str = 'long') -> None:
    """
    Append one trade event to the appropriate side's CSV log.
    side = 'long' → LOG_FILE_LONG
    side = 'short' → LOG_FILE_SHORT
    """
    log_file = LOG_FILE_LONG if side == 'long' else LOG_FILE_SHORT
    row = {col: '' for col in CSV_COLUMNS}
    row.update(data_dict)
    row['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pd.DataFrame([row], columns=CSV_COLUMNS).to_csv(
        log_file, mode='a', index=False,
        header=not os.path.exists(log_file)
    )


def log_status_to_csv(data_dict: dict) -> None:
    """Append BTC regime snapshot (shared between both sides)."""
    row = {col: '' for col in STATUS_COLUMNS}
    row.update(data_dict)
    row['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pd.DataFrame([row], columns=STATUS_COLUMNS).to_csv(
        STATUS_FILE, mode='a', index=False,
        header=not os.path.exists(STATUS_FILE)
    )


# ==========================================
# 🛠️ [MODULE 2] PnL Settlement (Dual-direction)
# ==========================================

def process_native_exit_log(symbol: str, pos: dict, side: str = 'long') -> float:
    """
    Reconstruct PnL for exchange-triggered exits (liquidation or native stop).
    Uses fetch_my_trades() — CCXT universal, works on Hyperliquid.

    [S-13] For SHORT positions, we look for 'buy' fills (covering the short).
           For LONG  positions, we look for 'sell' fills (closing the long).
    """
    real_exit_price = pos['entry_price']
    real_pnl        = 0.0
    close_side      = 'buy' if side == 'short' else 'sell'  # [S-13]

    try:
        recent_trades = exchange.fetch_my_trades(symbol, limit=5)
        close_fills   = [t for t in recent_trades if t.get('side', '') == close_side]

        if close_fills:
            last_fill       = close_fills[-1]
            real_exit_price = float(last_fill.get('price', pos['entry_price']))
            fill_fee        = float(last_fill.get('fee', {}).get('cost', 0) or 0)
            fill_amount     = float(last_fill.get('amount', pos['amount']))

            if side == 'long':
                real_pnl = round(
                    (real_exit_price - pos['entry_price']) * fill_amount - fill_fee, 4
                )
            else:
                # [S-4] Short PnL = (entry - exit) * qty - fees
                real_pnl = round(
                    (pos['entry_price'] - real_exit_price) * fill_amount - fill_fee, 4
                )
        else:
            raise ValueError(f"No {close_side} fills found")

    except Exception as e:
        logger.debug(f"⚠️ {symbol} PnL fetch failed, estimating: {e}")
        try:
            curr_p          = exchange.fetch_ticker(symbol)['last']
            real_exit_price = curr_p
            fee_est         = curr_p * pos['amount'] * 0.0007
            if side == 'long':
                real_pnl = round((curr_p - pos['entry_price']) * pos['amount'] - fee_est, 4)
            else:
                real_pnl = round((pos['entry_price'] - curr_p) * pos['amount'] - fee_est, 4)
        except Exception:
            pass

    log_to_csv({
        'symbol': symbol, 'action': 'NATIVE_EXIT', 'price': real_exit_price,
        'amount': pos['amount'], 'reason': 'HL Native Exit / Liquidation',
        'realized_pnl': real_pnl
    }, side=side)

    return real_pnl


# ==========================================
# 🛠️ [MODULE 3] Dynamic Ban System (JSON-Persistent)
# ==========================================

def _ban_key(symbol: str, side: str) -> str:
    """Generate a unique ban-tracker key that distinguishes long vs short."""
    return f"{symbol}_{side}"


def save_dynamic_blacklist() -> None:
    """Persist ban memory (cooldowns + loss counts) to JSON."""
    data = {
        'consecutive_losses': consecutive_losses,
        'cooldown_tracker':   cooldown_tracker
    }
    try:
        with open(BLACKLIST_FILE, 'w') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        logger.error(f"❌ 儲存黑名單 JSON 失敗: {e}")


def load_dynamic_blacklist() -> None:
    """Restore ban memory on startup."""
    global consecutive_losses, cooldown_tracker
    if os.path.exists(BLACKLIST_FILE):
        try:
            with open(BLACKLIST_FILE, 'r') as f:
                data = json.load(f)
                consecutive_losses.update(data.get('consecutive_losses', {}))
                cooldown_tracker.update(data.get('cooldown_tracker', {}))

            curr_t  = time.time()
            expired = [k for k, v in cooldown_tracker.items() if v < curr_t]
            for k in expired:
                del cooldown_tracker[k]
                if k in consecutive_losses:
                    del consecutive_losses[k]
            if expired:
                save_dynamic_blacklist()

            banned_count = sum(1 for v in cooldown_tracker.values() if v > curr_t + 3600)
            print(f"✅ JSON 記憶讀取成功！{banned_count} 隻幣處於 24H 封禁中。")
        except Exception as e:
            logger.error(f"❌ 讀取黑名單 JSON 失敗: {e}")
    else:
        print("ℹ️ 無歷史 JSON 記憶，全新啟動。")


def handle_trade_result(symbol: str, pnl: float, side: str = 'long') -> None:
    """
    Update consecutive-loss counter for a specific symbol+side pair.
    Win  → reset counter, remove cooldown.
    Loss → increment counter; ban 24h after MAX_CONSECUTIVE_LOSSES.
    """
    key = _ban_key(symbol, side)
    if pnl > 0:
        consecutive_losses[key] = 0
        print(f"🏆 {symbol} ({side}) 贏錢！解除冷卻。")
        if key in cooldown_tracker:
            del cooldown_tracker[key]
    elif pnl < 0:
        consecutive_losses[key] = consecutive_losses.get(key, 0) + 1
        if consecutive_losses[key] >= MAX_CONSECUTIVE_LOSSES:
            cooldown_tracker[key] = time.time() + DYNAMIC_BAN_DURATION
            print(
                f"🚫 [動態封禁] {symbol} ({side}) 連續虧損 "
                f"{consecutive_losses[key]} 次！封禁 24 小時。"
            )
        else:
            cooldown_tracker[key] = max(
                cooldown_tracker.get(key, 0),
                time.time() + 480
            )
    save_dynamic_blacklist()


def is_in_cooldown(symbol: str, side: str) -> bool:
    """Return True if this symbol+side combination is currently cooling down."""
    key = _ban_key(symbol, side)
    if key in cooldown_tracker:
        if time.time() < cooldown_tracker[key]:
            return True
        else:
            del cooldown_tracker[key]
    return False


# ==========================================
# 🛠️ [MODULE 4] Account & Order Utilities
# ==========================================

def get_live_usdc_balance() -> float:
    """Fetch free USDC balance from Hyperliquid."""
    try:
        bal = exchange.fetch_balance({'type': 'swap', 'user': API_KEY})
        usdc_free = (
            bal.get('USDC', {}).get('free', 0) or
            bal.get('total', {}).get('USDC', 0) or 0
        )
        return float(usdc_free)
    except Exception as e:
        logger.error(f"❌ 餘額查詢失敗: {e}")
        return 0.0


def cancel_all_hl(symbol: str) -> None:
    """Cancel all open orders for a symbol (both limit and trigger orders)."""
    try:
        exchange.cancel_all_orders(symbol)
        logger.debug(f"🧹 {symbol} 所有掛單已撤銷")
    except Exception as e:
        logger.debug(f"⚠️ {symbol} 撤單失敗 (non-critical): {e}")


def get_3_layer_avg_price(symbol: str, side: str = 'bids') -> float | None:
    """Average price of the top 3 order book levels on the specified side."""
    try:
        ob     = exchange.fetch_order_book(symbol, limit=5)
        levels = ob[side][:3]
        if not levels:
            return None
        return sum(lv[0] for lv in levels) / len(levels)
    except Exception:
        return None


def get_market_metrics(symbol: str) -> tuple[float | None, bool]:
    """
    ATR(14) on 5-minute candles.
    is_volatile = True when ATR/price > 0.15% (avoids fee-grinding).
    """
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe='5m', limit=50)
        df    = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
        df['tr'] = np.maximum(
            df['h'] - df['l'],
            np.maximum(
                abs(df['h'] - df['c'].shift(1)),
                abs(df['l'] - df['c'].shift(1))
            )
        )
        atr = df['tr'].rolling(14, min_periods=1).mean().iloc[-1]
        if pd.isna(atr) or atr == 0:
            return None, False
        return atr, (atr / df['c'].iloc[-1]) > 0.0015
    except Exception:
        return None, False


def _size_position(
    current_price: float, atr: float,
    sl_atr_mult: float, actual_bal: float
) -> tuple[float, float]:
    """
    Shared risk-based position sizer.
    Returns (trade_val_usdc, amount_in_contracts).
    Formula:
      dollar_risk   = effective_balance × RISK_PER_TRADE
      stop_distance = sl_atr_mult × ATR / price   (as a fraction)
      trade_val     = dollar_risk / stop_distance
      then capped at leverage limit and MAX_NOTIONAL_PER_TRADE.
    """
    eff_bal       = min(WORKING_CAPITAL, actual_bal)
    stop_frac     = (sl_atr_mult * atr) / current_price
    trade_val     = min(
        (eff_bal * RISK_PER_TRADE) / stop_frac,
        eff_bal * MAX_LEVERAGE * 0.95,
        MAX_NOTIONAL_PER_TRADE
    )
    return trade_val, eff_bal


# ==========================================
# 🧠 [MODULE 5] BTC Regime Navigator (Dual-Direction)
# ==========================================

def get_btc_regime() -> int:
    """
    BTC macro regime filter using HMA(20/50) crossover + ADX(14) + Volume.

    Returns:
       +1  = BULLISH  : HMA20 > HMA50, ADX > 22, vol confirmed → scout LONGS
        0  = NEUTRAL  : Partial confluence or mixed → no new entries
       -1  = BEARISH  : HMA20 < HMA50, ADX > 22, vol confirmed → scout SHORTS

    [S-12] Bearish signal: HMA20 < HMA50 (short HMA crosses below long HMA).
    Both bull and bear require ADX > 22 to filter sideways/choppy markets.
    ADX measures trend STRENGTH regardless of direction — an ADX > 22 in a
    downtrend is just as valid a signal as in an uptrend.
    """
    try:
        ohlcv  = exchange.fetch_ohlcv('BTC/USDC:USDC', timeframe='15m', limit=150)
        df     = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
        curr_p = df['c'].iloc[-1]

        # HMA(n) = WMA(sqrt(n), 2*WMA(n/2) - WMA(n))
        def calc_hma(series: pd.Series, period: int) -> pd.Series:
            half_l = int(period / 2)
            sqrt_l = int(np.sqrt(period))
            w_h = np.arange(1, half_l + 1)
            w_f = np.arange(1, period + 1)
            w_s = np.arange(1, sqrt_l + 1)
            wma_h = series.rolling(half_l).apply(lambda x: np.dot(x, w_h) / w_h.sum(), raw=True)
            wma_f = series.rolling(period).apply(lambda x: np.dot(x, w_f) / w_f.sum(), raw=True)
            return ((2 * wma_h - wma_f)
                    .rolling(sqrt_l)
                    .apply(lambda x: np.dot(x, w_s) / w_s.sum(), raw=True))

        df['hma20'], df['hma50'] = calc_hma(df['c'], 20), calc_hma(df['c'], 50)
        hma20_val = df['hma20'].iloc[-1]
        hma50_val = df['hma50'].iloc[-1]

        # Trend direction
        bullish = hma20_val > hma50_val   # Long regime
        bearish = hma20_val < hma50_val   # Short regime  [S-12]

        # ADX(14) — shared strength filter
        df['up']   = df['h'] - df['h'].shift(1)
        df['down'] = df['l'].shift(1) - df['l']
        df['+dm']  = np.where((df['up'] > df['down'])   & (df['up'] > 0),   df['up'],   0)
        df['-dm']  = np.where((df['down'] > df['up'])   & (df['down'] > 0), df['down'], 0)
        df['tr']   = np.maximum(
            df['h'] - df['l'],
            np.maximum(abs(df['h'] - df['c'].shift(1)), abs(df['l'] - df['c'].shift(1)))
        )
        atr_14   = df['tr'].ewm(alpha=1/14, adjust=False).mean()
        plus_di  = 100 * (pd.Series(df['+dm']).ewm(alpha=1/14, adjust=False).mean() / atr_14)
        minus_di = 100 * (pd.Series(df['-dm']).ewm(alpha=1/14, adjust=False).mean() / atr_14)
        denom    = plus_di + minus_di
        dx       = np.where(denom != 0, 100 * abs(plus_di - minus_di) / denom, 0)
        adx_val  = pd.Series(dx).ewm(alpha=1/14, adjust=False).mean().iloc[-1]
        cond_adx = adx_val > 22

        # Volume confirmation (use last fully-closed candle, not forming one)
        completed_v = df['v'].iloc[-2]
        median_v    = df['v'].iloc[-25:-1].median()
        cond_vol    = completed_v > median_v * 0.8

        # Signal assembly
        if bullish and cond_adx and cond_vol:
            signal, status = 1,  f"🟢 BULLISH  (HMA20↑ ADX:{adx_val:.1f} Vol✅)"
        elif bearish and cond_adx and cond_vol:
            signal, status = -1, f"🔴 BEARISH  (HMA20↓ ADX:{adx_val:.1f} Vol✅)"
        elif (bullish or bearish) and (cond_adx or cond_vol):
            signal, status = 0,  f"🟡 NEUTRAL  (待確認 ADX:{adx_val:.1f})"
        else:
            signal, status = 0,  f"⬜ SIDEWAYS (No trend ADX:{adx_val:.1f})"

        log_status_to_csv({
            'btc_price':    round(curr_p, 2),
            'target_price': round(hma50_val, 2),
            'hma20':        round(hma20_val, 2),
            'hma50':        round(hma50_val, 2),
            'adx':          round(adx_val, 2),
            'signal_code':  signal,
            'decision_text': status
        })

        dir_arrow = "▲" if bullish else ("▼" if bearish else "—")
        print("-" * 65)
        print(f"📊 BTC 戰報 | 現價: {curr_p:.0f} USDC | HMA20/50: {hma20_val:.0f}/{hma50_val:.0f} {dir_arrow}")
        print(f"   ADX: {adx_val:.1f} {'✅' if cond_adx else '❌'} | "
              f"Vol: {completed_v:.0f} vs {median_v*0.8:.0f} {'✅' if cond_vol else '❌'}")
        print(f"   🚦 {status}")
        print("-" * 65)

        return signal

    except Exception as e:
        print(f"⚠️ 導航故障: {e}")
        return 0


# ==========================================
# 🧠 [MODULE 6] Coin Scouts (Long + Short)
# ==========================================

def scouting_strong_coins(n: int = 20) -> list[str]:
    """
    Long scout: Top-N by volume, then ranked by strongest 24h % gain.
    Tight spread filter (<0.1%) ensures sufficient liquidity.
    """
    return _scout_universe(n, ascending=False)


def scouting_weak_coins(n: int = 20) -> list[str]:
    """
    [S-11] Short scout: Top-N by volume, then ranked by weakest 24h % change.
    We want coins with the most negative momentum (biggest losers in liquid pool).
    ascending=True → most negative change first.
    """
    return _scout_universe(n, ascending=True)


def _scout_universe(n: int, ascending: bool) -> list[str]:
    """
    Shared market scan kernel.
    ascending=False → strongest movers (LONG signal)
    ascending=True  → weakest  movers (SHORT signal)
    """
    try:
        tickers = exchange.fetch_tickers()
        data    = []
        for s, t in tickers.items():
            if not s.endswith(':USDC'):
                continue
            if s in BLACKLIST:
                continue
            if t.get('percentage') is None:
                continue
            ask, bid = t.get('ask'), t.get('bid')
            if not (ask and bid and bid > 0):
                continue
            if (ask - bid) / bid < 0.0010:  # Tight spread gate
                data.append({
                    'symbol': s,
                    'volume': t.get('quoteVolume', 0) or 0,
                    'change': t['percentage']
                })

        if not data:
            return []

        df         = pd.DataFrame(data)
        top_liquid = df.sort_values('volume', ascending=False).head(n)
        return (top_liquid
                .sort_values('change', ascending=ascending)
                .head(n)['symbol']
                .tolist())

    except Exception as e:
        print(f"⚠️ Scout Error: {e}")
        return []


# ==========================================
# 🧠 [MODULE 7] Flow Health Radars
# ==========================================

def check_flow_health_long(symbol: str) -> str | None:
    """
    LONG defence: detect extreme sell dumps or momentum deceleration.
    Returns string reason or None.
    Exit triggers:
      • Z < -3.0                              → "Flow Reversal (Long Dump Detected)"
      • accel_z < -2.5 AND flow<0 AND asks↑  → "Flow Deceleration (Momentum Died)"
    """
    return _check_flow_health(symbol, direction='long')


def check_flow_health_short(symbol: str) -> str | None:
    """
    [S-7/S-8] SHORT defence: detect extreme buy squeezes or upward deceleration.
    Returns string reason or None.
    Exit triggers:
      • Z > +3.0                              → "Flow Reversal (Short Squeeze Detected)"
      • accel_z > +2.5 AND flow>0 AND bids↑  → "Flow Deceleration (Upward Momentum)"
    """
    return _check_flow_health(symbol, direction='short')


def _check_flow_health(symbol: str, direction: str) -> str | None:
    """
    Shared flow health kernel parameterised by direction.
    direction='long'  → guard against sell pressure
    direction='short' → guard against buy pressure  [S-7]
    """
    try:
        trades = exchange.fetch_trades(symbol, limit=100)
        if not trades or len(trades) < 50:
            return None

        df               = pd.DataFrame(trades)
        df['price_change'] = df['price'].diff()
        df['direction']    = np.where(
            df['price_change'] > 0, 1, np.where(df['price_change'] < 0, -1, 0)
        )
        df['direction'] = df['direction'].replace(0, np.nan).ffill().fillna(0)

        avg_vol        = df['amount'].mean()
        df['weight']   = np.where(df['amount'] > avg_vol * 2, 2.0, 1.0)
        df['net_flow'] = df['direction'] * df['amount'] * df['price'] * df['weight']

        flow_std = df['net_flow'].std()
        if flow_std == 0:
            return None

        flow_mean      = df['net_flow'].mean()
        recent_25_flow = df['net_flow'].tail(25).sum()
        z_score = (recent_25_flow - (flow_mean * 25)) / (flow_std * np.sqrt(25))

        if direction == 'long':
            # Long: extreme sell pressure is the threat
            if z_score < -3.0:
                return "Flow Reversal (Long Dump Detected)"
            # Deceleration: downward acceleration + flow turning negative
            flow_older = df['net_flow'].iloc[-50:-25].sum()
            accel_z    = (recent_25_flow - flow_older) / (flow_std * np.sqrt(25))
            # [HL-11] threshold -2.5 (tighter than Bybit -2.0 due to HL high-freq noise)
            if accel_z < -2.5 and recent_25_flow < 0:
                ob = exchange.fetch_order_book(symbol, limit=20)
                bv = sum(b[1] for b in ob['bids'])
                av = sum(a[1] for a in ob['asks'])
                if (bv + av) > 0 and (bv - av) / (bv + av) < -0.15:
                    return "Flow Deceleration (Momentum Died)"

        else:  # direction == 'short'
            # [S-7] Short: extreme buy pressure (squeeze) is the threat
            if z_score > 3.0:
                return "Flow Reversal (Short Squeeze Detected)"
            # [S-8] Deceleration: upward acceleration + flow turning positive
            flow_older = df['net_flow'].iloc[-50:-25].sum()
            accel_z    = (recent_25_flow - flow_older) / (flow_std * np.sqrt(25))
            # Threshold +2.5 (symmetric with long, HL noise-adjusted)
            if accel_z > 2.5 and recent_25_flow > 0:
                try:
                    ob = exchange.fetch_order_book(symbol, limit=20)
                    bv = sum(b[1] for b in ob['bids'])
                    av = sum(a[1] for a in ob['asks'])
                    if (bv + av) > 0 and (bv - av) / (bv + av) > 0.15:
                        # Bids dominating confirms upward momentum revival
                        return "Flow Deceleration (Upward Momentum)"
                except Exception:
                    pass

        return None

    except Exception:
        return None


# ==========================================
# 🧠 [MODULE 8] Lee-Ready Entry Snipers
# ==========================================

def apply_lee_ready_long_logic(symbol: str) -> tuple[float, float, bool]:
    """
    Long entry: net buy flow + upward acceleration + bid-heavy OB.
    Anti-fake-breakout: cancel if asks dominate despite price pump.
    Returns: (net_flow, last_price, is_strong)
    """
    return _lee_ready_logic(symbol, direction='long')


def apply_lee_ready_short_logic(symbol: str) -> tuple[float, float, bool]:
    """
    [S-9/S-10] Short entry: net sell flow + downward acceleration + ask-heavy OB.
    Anti-squeeze filter: cancel if bids dominate despite price dump.
    Returns: (net_flow, last_price, is_weak)
    """
    return _lee_ready_logic(symbol, direction='short')


def _lee_ready_logic(symbol: str, direction: str) -> tuple[float, float, bool]:
    """
    Shared Lee-Ready flow classification kernel.
    direction='long'  → looking for buy pressure
    direction='short' → looking for sell pressure  [S-9]
    """
    try:
        trades = exchange.fetch_trades(symbol, limit=200)
        if not trades:
            return 0, 0, False

        df               = pd.DataFrame(trades)
        df['price_change'] = df['price'].diff()
        df['direction']    = np.where(
            df['price_change'] > 0, 1, np.where(df['price_change'] < 0, -1, 0)
        )
        df['direction'] = df['direction'].replace(0, np.nan).ffill().fillna(0)

        avg_vol        = df['amount'].mean()
        df['weight']   = np.where(df['amount'] > avg_vol * 2, 2.0, 1.0)
        df['net_flow'] = df['direction'] * df['amount'] * df['price'] * df['weight']

        flow_w50 = df['net_flow'].tail(50).sum()
        accel    = df['net_flow'].tail(25).sum() - df['net_flow'].iloc[-50:-25].sum()

        try:
            ob       = exchange.fetch_order_book(symbol, limit=20)
            bv       = sum(b[1] for b in ob['bids'])
            av       = sum(a[1] for a in ob['asks'])
            imbal    = (bv - av) / (bv + av) if (bv + av) > 0 else 0
        except Exception:
            imbal = 0

        flow_std = df['net_flow'].std()
        z_score  = flow_w50 / (flow_std * np.sqrt(50)) if flow_std > 0 else 0

        signal = False

        if direction == 'long':
            # Condition A: Triple confluence (buy pressure)
            if flow_w50 > 0 and accel > 0 and imbal > 0.15:
                signal = True
                print(f"🔥 {symbol} LONG Sniper! Accel:{accel:.0f} OB:{imbal:.2f}")
            # Condition B: Z-score alone
            elif z_score > NET_FLOW_SIGMA:
                signal = True
                print(f"📈 {symbol} LONG Z:{z_score:.2f}")
            # Anti-fake-breakout: asks dominate → abort
            if signal and imbal < -0.1:
                signal = False
                print(f"⚠️ {symbol} 假突破攔截！賣盤主導，取消做多。")

        else:  # short
            # [S-9] Condition A: Triple confluence (sell pressure)
            if flow_w50 < 0 and accel < 0 and imbal < -0.15:
                signal = True
                print(f"🔥 {symbol} SHORT Sniper! Accel:{accel:.0f} OB:{imbal:.2f}")
            # Condition B: negative Z-score
            elif z_score < -NET_FLOW_SIGMA:
                signal = True
                print(f"📉 {symbol} SHORT Z:{z_score:.2f}")
            # [S-10] Anti-squeeze: bids dominate → abort short
            if signal and imbal > 0.1:
                signal = False
                print(f"⚠️ {symbol} 擠空陷阱攔截！買盤主導，取消做空。")

        return flow_w50, df['price'].iloc[-1], signal

    except Exception as e:
        print(f"⚠️ LR Error [{symbol}]: {e}")
        return 0, 0, False


# ==========================================
# 🛡️ [MODULE 9] Startup Position Sync
# ==========================================

def sync_positions_on_startup() -> None:
    """
    On restart: adopt any existing open positions from exchange.
    Separates longs into long_positions and shorts into short_positions.
    """
    print("🔄 同步 Hyperliquid 現有倉位...")
    try:
        raw = exchange.fetch_positions(None, {'user': API_KEY})
        long_count = short_count = 0

        for p in raw:
            size = float(p.get('contracts', 0) or p.get('info', {}).get('szi', 0) or 0)
            if size == 0:
                continue

            symbol = p['symbol']
            side   = p.get('side', '').lower()

            entry_price = float(
                p.get('entryPrice') or p.get('info', {}).get('entryPx', 0) or 0
            )
            if entry_price == 0:
                continue

            atr, _ = get_market_metrics(symbol)
            if not atr:
                atr = entry_price * 0.01

            if side in ['long', 'buy']:
                sl_p = float(exchange.price_to_precision(
                    symbol, entry_price - (SL_ATR_MULT_LONG * atr)
                ))
                tp_p = float(exchange.price_to_precision(
                    symbol, entry_price + (TP_ATR_MULT_LONG * atr)
                ))
                is_be = sl_p > entry_price
                long_positions[symbol] = {
                    'amount': size, 'entry_price': entry_price,
                    'tp_price': tp_p, 'sl_price': sl_p,
                    'is_breakeven': is_be, 'atr': atr, 'max_pnl_pct': 0.0,
                    'entry_time': time.time(), 'deceleration_detected': False,
                }
                long_count += 1
                print(f"✅ 恢復多單: {symbol} @ {entry_price:.4f}")

            elif side in ['short', 'sell']:
                # [S-3] Short SL is above entry, TP is below entry
                sl_p = float(exchange.price_to_precision(
                    symbol, entry_price + (SL_ATR_MULT_SHORT * atr)
                ))
                tp_p = float(exchange.price_to_precision(
                    symbol, entry_price - (TP_ATR_MULT_SHORT * atr)
                ))
                # [S-5] Short breakeven: SL moved below entry (locked profit)
                is_be = (sl_p < entry_price and sl_p > 0)
                short_positions[symbol] = {
                    'amount': size, 'entry_price': entry_price,
                    'tp_price': tp_p, 'sl_price': sl_p,
                    'is_breakeven': is_be, 'atr': atr, 'max_pnl_pct': 0.0,
                    'entry_time': time.time(), 'deceleration_detected': False,
                }
                short_count += 1
                print(f"✅ 恢復空單: {symbol} @ {entry_price:.4f}")

        print(f"🔄 同步完成！多單: {long_count} | 空單: {short_count}")

    except Exception as e:
        logger.error(f"❌ 啟動同步失敗: {e}")


# ==========================================
# 🛡️ [MODULE 10A] Long Position Manager
# ==========================================

def manage_long_positions() -> None:
    """
    Manage all open LONG positions.
    Trail SL advances UPWARD as price rises.
    Exit: TP hit (price >= tp_price) or SL hit (price <= sl_price).
    """
    _manage_positions(side='long')


# ==========================================
# 🛡️ [MODULE 10B] Short Position Manager
# ==========================================

def manage_short_positions() -> None:
    """
    [S-4 to S-8] Manage all open SHORT positions.
    Trail SL advances DOWNWARD as price falls.
    Exit: TP hit (price <= tp_price) or SL hit (price >= sl_price).
    """
    _manage_positions(side='short')


def _manage_positions(side: str) -> None:
    """
    Shared position management kernel for both Long and Short.
    Parameterised by side to handle the mirrored arithmetic.
    """
    pos_dict = long_positions if side == 'long' else short_positions

    try:
        live_raw = exchange.fetch_positions()
        live_map = {}
        for p in live_raw:
            sz = float(p.get('contracts', 0) or p.get('info', {}).get('szi', 0) or 0)
            if sz > 0:
                live_map[p['symbol']] = p

        # ── Step 1: Orphan adoption ──────────────────────────────────
        for sym, p in live_map.items():
            if sym in pos_dict:
                continue
            p_side = p.get('side', '').lower()
            if side == 'long'  and p_side not in ['long', 'buy']:
                continue
            if side == 'short' and p_side not in ['short', 'sell']:
                continue

            entry_p = float(p.get('entryPrice') or p.get('info', {}).get('entryPx', 0) or 0)
            amt     = float(p.get('contracts', 0) or p.get('info', {}).get('szi', 0) or 0)
            if entry_p == 0 or amt == 0:
                continue

            atr, _ = get_market_metrics(sym)
            if not atr:
                atr = entry_p * 0.01

            raw_ts       = p.get('createdTime') or p.get('info', {}).get('time')
            real_entry_t = float(raw_ts) / 1000.0 if raw_ts else time.time()

            if side == 'long':
                sl_p = float(exchange.price_to_precision(sym, entry_p - (SL_ATR_MULT_LONG  * atr)))
                tp_p = float(exchange.price_to_precision(sym, entry_p + (TP_ATR_MULT_LONG  * atr)))
                is_be = sl_p > entry_p
            else:
                # [S-3] Short: SL above, TP below
                sl_p  = float(exchange.price_to_precision(sym, entry_p + (SL_ATR_MULT_SHORT * atr)))
                tp_p  = float(exchange.price_to_precision(sym, entry_p - (TP_ATR_MULT_SHORT * atr)))
                is_be = (sl_p < entry_p and sl_p > 0)   # [S-5]

            pos_dict[sym] = {
                'amount': amt, 'entry_price': entry_p, 'tp_price': tp_p,
                'sl_price': sl_p, 'is_breakeven': is_be, 'atr': atr,
                'max_pnl_pct': 0.0, 'entry_time': real_entry_t,
                'deceleration_detected': False,
            }
            print(f"🚨 [自癒接管] 孤兒{side}單: {sym} @ {entry_p:.4f} amt:{amt}")

        # ── Step 2: Detect exchange-closed positions ─────────────────
        for sym in list(pos_dict.keys()):
            if sym not in live_map:
                print(f"🧹 交易所已平{side}倉，結算 PnL: {sym}")
                real_pnl = process_native_exit_log(sym, pos_dict[sym], side=side)
                cancel_all_hl(sym)
                handle_trade_result(sym, real_pnl, side=side)
                del pos_dict[sym]
                continue

        # ── Step 3-6: Manage live positions ──────────────────────────
        for sym in list(pos_dict.keys()):
            pos    = pos_dict[sym]
            curr_p = exchange.fetch_ticker(sym)['last']

            if side == 'long':
                pnl_pct = (curr_p - pos['entry_price']) / pos['entry_price']
            else:
                # [S-4] Short PnL: positive when price falls
                pnl_pct = (pos['entry_price'] - curr_p) / pos['entry_price']

            coin_vol_pct = pos['atr'] / pos['entry_price']
            sl_updated   = False

            pos['max_pnl_pct'] = max(pos.get('max_pnl_pct', pnl_pct), pnl_pct)

            # ── Breakeven push ────────────────────────────────────────
            if not pos['is_breakeven'] and pnl_pct > (coin_vol_pct * 2.0):
                if side == 'long':
                    pos['sl_price'] = pos['entry_price'] * 1.002  # SL moves up
                else:
                    # [S-5] Short breakeven: SL moves DOWN to entry * 0.998
                    pos['sl_price'] = pos['entry_price'] * 0.998
                pos['is_breakeven'] = True
                sl_updated          = True

            # ── Variable trail SL ─────────────────────────────────────
            if pos['is_breakeven']:
                if side == 'long':
                    # Trail SL below current price, tightens with profit depth
                    if pos.get('deceleration_detected') and pnl_pct > (coin_vol_pct * 2.5):
                        trail_sl = curr_p - (0.5 * pos['atr'])
                    elif pnl_pct > (coin_vol_pct * 5.0):
                        trail_sl = curr_p - (0.8 * pos['atr'])
                    elif pnl_pct > (coin_vol_pct * 3.5):
                        trail_sl = curr_p - (1.2 * pos['atr'])
                    else:
                        trail_sl = curr_p - (1.8 * pos['atr'])

                    # Long trail SL can only move UP
                    if trail_sl > pos['sl_price']:
                        if (trail_sl - pos['sl_price']) / pos['sl_price'] > 0.0005:
                            sl_updated      = True
                            pos['sl_price'] = trail_sl

                else:  # short
                    # [S-6] Short trail SL is ABOVE current price, moves DOWN as price falls
                    if pos.get('deceleration_detected') and pnl_pct > (coin_vol_pct * 2.5):
                        trail_sl = curr_p + (0.5 * pos['atr'])
                    elif pnl_pct > (coin_vol_pct * 5.0):
                        trail_sl = curr_p + (0.8 * pos['atr'])
                    elif pnl_pct > (coin_vol_pct * 3.5):
                        trail_sl = curr_p + (1.2 * pos['atr'])
                    else:
                        trail_sl = curr_p + (1.8 * pos['atr'])

                    # Short trail SL can only move DOWN (lower stop = more locked profit)
                    if trail_sl < pos['sl_price']:
                        if (pos['sl_price'] - trail_sl) / pos['sl_price'] > 0.0005:
                            sl_updated      = True
                            pos['sl_price'] = trail_sl

            if sl_updated:
                logger.debug(f"📐 {sym} ({side}) Trail SL → {pos['sl_price']:.4f} | PnL {pnl_pct*100:.2f}%")

            # ── Zombie timeout ────────────────────────────────────────
            exit_reason = None
            time_held   = time.time() - pos.get('entry_time', time.time())
            if time_held > 2700 and pnl_pct < 0.005:
                exit_reason = "Momentum Timeout (Stalled Zombie)"

            # ── Flow health radar ─────────────────────────────────────
            curr_t     = time.time()
            last_check = pos.get('last_flow_check', 0)
            if not exit_reason and (curr_t - last_check > 15):
                pos['last_flow_check'] = curr_t
                if time_held > 120:
                    if side == 'long':
                        flow_status = check_flow_health_long(sym)
                        reversal_tag    = "Flow Reversal (Long Dump Detected)"
                        decel_tag       = "Flow Deceleration (Momentum Died)"
                    else:
                        flow_status = check_flow_health_short(sym)
                        reversal_tag    = "Flow Reversal (Short Squeeze Detected)"
                        decel_tag       = "Flow Deceleration (Upward Momentum)"

                    if flow_status == reversal_tag:
                        exit_reason = flow_status
                    elif flow_status == decel_tag:
                        if not pos.get('deceleration_detected', False):
                            pos['deceleration_detected'] = True
                            print(f"⚠️ {sym} ({side}) 動能衰退標記已啟動！Trail SL 收緊至 0.5 ATR。")

            # ── Local TP/SL check ─────────────────────────────────────
            if not exit_reason:
                if side == 'long':
                    if curr_p >= pos['tp_price']:
                        exit_reason = "TP (Long IOC Exit)"
                    elif curr_p <= pos['sl_price']:
                        exit_reason = ("Trail SL (Long IOC Exit)"
                                       if pos['is_breakeven'] else "SL (Long IOC Exit)")
                else:
                    # [S-2/S-3] Short: TP is below, SL is above
                    if curr_p <= pos['tp_price']:
                        exit_reason = "TP (Short IOC Exit)"
                    elif curr_p >= pos['sl_price']:
                        exit_reason = ("Trail SL (Short IOC Exit)"
                                       if pos['is_breakeven'] else "SL (Short IOC Exit)")

            # ── Execute exit ──────────────────────────────────────────
            if exit_reason:
                print(
                    f"⚔️ {sym} ({side}) 觸發 {exit_reason} | "
                    f"持倉:{time_held/60:.1f}m | "
                    f"MaxPnL:{pos['max_pnl_pct']*100:.2f}% | "
                    f"PnL:{pnl_pct*100:.2f}%"
                )

                if side == 'long':
                    # Close long: sell at bid
                    ioc_price = get_3_layer_avg_price(sym, 'bids') or curr_p
                    order_side, fallback = 'sell', exchange.create_market_sell_order
                else:
                    # [S-1] Close short: buy at ask
                    ioc_price = get_3_layer_avg_price(sym, 'asks') or curr_p
                    order_side, fallback = 'buy', exchange.create_market_buy_order

                try:
                    exchange.create_order(
                        sym, 'limit', order_side, pos['amount'], ioc_price,
                        {'timeInForce': 'IOC', 'reduceOnly': True}
                    )
                except Exception:
                    fallback(sym, pos['amount'], {'reduceOnly': True})

                if side == 'long':
                    ioc_pnl = round((ioc_price - pos['entry_price']) * pos['amount'], 4)
                    action  = 'LONG_EXIT'
                else:
                    # [S-4]
                    ioc_pnl = round((pos['entry_price'] - ioc_price) * pos['amount'], 4)
                    action  = 'SHORT_EXIT'

                log_to_csv({
                    'symbol': sym, 'action': action, 'price': curr_p,
                    'amount': pos['amount'], 'reason': exit_reason,
                    'realized_pnl': ioc_pnl
                }, side=side)

                cancel_all_hl(sym)
                handle_trade_result(sym, ioc_pnl, side=side)
                del pos_dict[sym]

    except ccxt.RateLimitExceeded:
        logger.warning(f"⏳ Rate limit hit ({side} manager) — sleeping 5s")
        time.sleep(5)
    except Exception as e:
        logger.error(f"❌ manage_{side}_positions 異常: {e}")


# ==========================================
# 🚀 [MODULE 11] Entry Executors
# ==========================================

def execute_live_long(
    symbol: str, net_flow: float,
    current_price: float, is_strong: bool,
    atr: float | None, is_volatile: bool
) -> None:
    """Open a new LONG position via IOC buy order."""
    _execute_entry(
        symbol, net_flow, current_price, is_strong, atr, is_volatile,
        side='long'
    )


def execute_live_short(
    symbol: str, net_flow: float,
    current_price: float, is_weak: bool,
    atr: float | None, is_volatile: bool
) -> None:
    """
    [S-1] Open a new SHORT position via IOC sell order.
    is_weak → confirmed downward flow signal from Lee-Ready short logic.
    """
    _execute_entry(
        symbol, net_flow, current_price, is_weak, atr, is_volatile,
        side='short'
    )


def _execute_entry(
    symbol: str, net_flow: float,
    current_price: float, signal: bool,
    atr: float | None, is_volatile: bool,
    side: str
) -> None:
    """
    Shared entry executor. Handles sizing, IOC order placement, and
    position registration for both long and short sides.
    """
    pos_dict  = long_positions if side == 'long' else short_positions
    tp_mult   = TP_ATR_MULT_LONG   if side == 'long' else TP_ATR_MULT_SHORT
    sl_mult   = SL_ATR_MULT_LONG   if side == 'long' else SL_ATR_MULT_SHORT

    # Check dynamic ban
    if is_in_cooldown(symbol, side):
        return

    if atr is None or atr == 0 or current_price == 0:
        return
    if not (signal and is_volatile and symbol not in pos_dict):
        return

    cancel_all_hl(symbol)

    actual_bal = get_live_usdc_balance()
    trade_val, eff_bal = _size_position(current_price, atr, sl_mult, actual_bal)
    amount = float(exchange.amount_to_precision(symbol, trade_val / current_price))

    min_amount = exchange.markets[symbol]['limits']['amount'].get('min', 0)
    if amount < min_amount:
        return

    # Entry price: top-3 ask avg for longs, top-3 bid avg for shorts
    if side == 'long':
        ioc_p      = get_3_layer_avg_price(symbol, 'asks') or current_price
        order_side = 'buy'
    else:
        # [S-1] Short entry: hit the bid
        ioc_p      = get_3_layer_avg_price(symbol, 'bids') or current_price
        order_side = 'sell'

    if amount * ioc_p < MIN_NOTIONAL:
        return

    try:
        exchange.set_leverage(int(MAX_LEVERAGE), symbol)
    except Exception as e:
        logger.warning(f"⚠️ {symbol} 槓桿設置: {e}")

    try:
        order = exchange.create_order(
            symbol, 'limit', order_side, amount, ioc_p,
            {'timeInForce': 'IOC'}
        )
        time.sleep(1)

        actual_price, actual_amount = ioc_p, 0.0

        try:
            detail        = exchange.fetch_order(order['id'], symbol, params={"acknowledged": True})
            actual_price  = float(detail.get('average') or detail.get('price') or ioc_p)
            actual_amount = float(detail.get('filled', 0))
        except Exception as e:
            logger.warning(f"⚠️ {symbol} 訂單確認失敗，備用同步: {e}")
            time.sleep(0.5)
            for p in exchange.fetch_positions():
                psz = float(p.get('contracts', 0) or p.get('info', {}).get('szi', 0) or 0)
                if p['symbol'] == symbol and psz > 0:
                    actual_amount = psz
                    actual_price  = float(p.get('entryPrice') or p.get('info', {}).get('entryPx', ioc_p) or ioc_p)
                    break

        if actual_amount == 0:
            print(f"⏩ {symbol} ({side}) IOC 未成交，撤單退出。")
            cancel_all_hl(symbol)
            return

        # TP and SL levels
        if side == 'long':
            tp_p = float(exchange.price_to_precision(symbol, actual_price + (tp_mult * atr)))
            sl_p = float(exchange.price_to_precision(symbol, actual_price - (sl_mult * atr)))
            profit_margin = (tp_p - actual_price) / actual_price
            emergency_close = exchange.create_market_sell_order
        else:
            # [S-2/S-3]
            tp_p = float(exchange.price_to_precision(symbol, actual_price - (tp_mult * atr)))
            sl_p = float(exchange.price_to_precision(symbol, actual_price + (sl_mult * atr)))
            profit_margin = (actual_price - tp_p) / actual_price
            emergency_close = exchange.create_market_buy_order

        # Minimum profit check: must cover ~2× round-trip fees (HL ~0.07%)
        if profit_margin < 0.003:
            print(f"🟡 放棄{side} [{symbol}]: 利潤空間({profit_margin*100:.2f}%) 不足，立即平倉！")
            try:
                emergency_close(symbol, actual_amount, {'reduceOnly': True})
            except Exception as e:
                logger.error(f"❌ {symbol} 緊急平倉失敗: {e}")
            cancel_all_hl(symbol)
            return

        print(f"✅ {symbol} ({side}) TP:{tp_p:.4f} | SL:{sl_p:.4f} [本地追蹤]")

        # Register in memory
        pos_dict[symbol] = {
            'amount':      actual_amount,
            'entry_price': actual_price,
            'tp_price':    tp_p,
            'sl_price':    sl_p,
            'is_breakeven':       False,
            'atr':                atr,
            'max_pnl_pct':        0.0,
            'entry_time':         time.time(),
            'deceleration_detected': False,
        }
        # Standard 8-minute cooldown after entry
        bk = _ban_key(symbol, side)
        cooldown_tracker[bk] = time.time() + 480
        save_dynamic_blacklist()

        action = 'LONG_ENTRY' if side == 'long' else 'SHORT_ENTRY'
        log_to_csv({
            'symbol':           symbol,
            'action':           action,
            'price':            actual_price,
            'amount':           actual_amount,
            'trade_value':      round(actual_amount * actual_price, 2),
            'atr':              round(atr, 4),
            'net_flow':         round(net_flow, 2),
            'tp_price':         tp_p,
            'sl_price':         sl_p,
            'actual_balance':   round(actual_bal, 2),
            'effective_balance': eff_bal
        }, side=side)

        emoji = "📈" if side == 'long' else "📉"
        print(f"{emoji} [已入{side}] {symbol} @ {actual_price:.4f} | qty:{actual_amount}")

    except ccxt.RateLimitExceeded:
        logger.warning(f"⏳ {symbol} ({side}) Rate Limit — retry later")
        time.sleep(5)
    except Exception as e:
        logger.error(f"❌ {symbol} ({side}) 入場失敗: {e}")


# ==========================================
# 🚀 [MODULE 12] Main Event Loop
# ==========================================

def main() -> None:
    print("=" * 65)
    print("🚀 Hyperliquid Dual-Side V1.0 啟動")
    print("   Long + Short | Lee-Ready Flow | AI Variable Trail SL")
    print("   JSON Ban System | USDC | HL Native")
    print("=" * 65)

    load_dynamic_blacklist()
    sync_positions_on_startup()

    last_scout_time = 0
    long_targets    = []
    short_targets   = []

    while True:
        try:
            # ── Inner loop: manage both sides every cycle ──────────────
            manage_long_positions()
            manage_short_positions()
            curr_t = time.time()

            # ── Outer loop: regime check + scouting ───────────────────
            if curr_t - last_scout_time > SCOUTING_INTERVAL:
                regime = get_btc_regime()

                if regime == 1:
                    # BULLISH → scout longs only
                    print("🟢 牛市確認！執行多單海選...")
                    long_targets  = scouting_strong_coins(20)
                    short_targets = []   # Flush stale short candidates

                    for s in long_targets:
                        try:
                            flow, last_p, is_strong = apply_lee_ready_long_logic(s)
                            atr, is_v               = get_market_metrics(s)
                            if last_p > 0:
                                execute_live_long(s, flow, last_p, is_strong, atr, is_v)
                        except Exception:
                            continue
                        time.sleep(0.5)

                elif regime == -1:
                    # BEARISH → scout shorts only
                    print("🔴 熊市確認！執行空單海選...")
                    short_targets = scouting_weak_coins(20)
                    long_targets  = []   # Flush stale long candidates

                    for s in short_targets:
                        try:
                            flow, last_p, is_weak = apply_lee_ready_short_logic(s)
                            atr, is_v             = get_market_metrics(s)
                            if last_p > 0:
                                execute_live_short(s, flow, last_p, is_weak, atr, is_v)
                        except Exception:
                            continue
                        time.sleep(0.5)

                else:
                    # NEUTRAL → no new entries, manage existing only
                    print(f"🟡 大盤中性 (regime={regime})，暫停海選，守倉管理中...")
                    long_targets  = []
                    short_targets = []

                last_scout_time = curr_t
                bal = get_live_usdc_balance()
                print(
                    f"⏳ 巡邏完畢 | 多倉:{list(long_positions.keys())} | "
                    f"空倉:{list(short_positions.keys())} | 餘額:{bal:.2f} USDC"
                )

            time.sleep(POSITION_CHECK_INTERVAL)

        except KeyboardInterrupt:
            bal = get_live_usdc_balance()
            print(
                f"\n👋 手動終止。餘額:{bal:.2f} USDC | "
                f"多倉:{list(long_positions.keys())} | "
                f"空倉:{list(short_positions.keys())}"
            )
            sys.exit(0)

        except ccxt.RateLimitExceeded:
            logger.warning("⏳ 主迴圈 Rate Limit — 等待 10s")
            time.sleep(10)

        except Exception as e:
            logger.error(f"❌ 主迴圈異常: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()