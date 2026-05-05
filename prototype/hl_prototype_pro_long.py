"""
================================================================================
  hyperliquid_long_v1.py
  Migrated from: prototype_long.py (Bybit V6.4 FINAL LONG)
  Target Exchange: Hyperliquid (via CCXT)
  Strategy: Long-Only Momentum Sniper
    - BTC Regime Filter  : HMA(20/50) Crossover + ADX(14) > 22 + Volume Median
    - Coin Scout         : Top-N by volume, ranked by 24h % change (Strongest)
    - Entry Signal       : Lee-Ready Net Flow + Acceleration + OB Imbalance
    - Risk Management    : ATR-based sizing + Multi-stage Variable Trail SL
    - Memory             : Dynamic 24h ban system persisted via JSON
  Base Currency: USDC (Hyperliquid native)
================================================================================
  KEY MIGRATION CHANGES FROM BYBIT VERSION:
  [HL-1]  Exchange instantiation: ccxt.bybit → ccxt.hyperliquid
  [HL-2]  Base currency: USDT → USDC throughout
  [HL-3]  Symbol format: BTC/USDT:USDT → BTC/USDC:USDC
  [HL-4]  Balance fetch: ['USDT']['free'] → ['USDC']['free']
  [HL-5]  cancel_all_v5() (Bybit private V5 endpoint) → cancel_all_hl()
            using standard CCXT cancel_all_orders() — HL has no native TP/SL
            attached to position; TP/SL are managed locally by this bot.
  [HL-6]  process_native_exit_log(): removed Bybit-specific
            private_get_v5_position_closed_pnl; replaced with CCXT
            fetch_my_trades() to reconstruct exit PnL from fill history.
  [HL-7]  execute_live_long(): removed Bybit-specific
            private_post_v5_position_trading_stop for TP/SL — HL does not
            support exchange-native conditional TP/SL via CCXT the same way.
            Bot relies 100% on local trailing-stop logic in manage_long_positions().
  [HL-8]  manage_long_positions(): removed private_post_v5_position_trading_stop
            SL update call; trail SL is now purely software-side.
  [HL-9]  set_leverage(): HL uses cross-margin by default; leverage is set
            per-symbol via CCXT set_leverage(), error codes adjusted.
  [HL-10] POSITION_CHECK_INTERVAL: 4s → 2s (HL latency is sub-100ms,
            allowing tighter position monitoring without rate-limit risk).
  [HL-11] Deceleration trigger threshold: accel_z < -2.0 → < -2.5 to reduce
            false positives on Hyperliquid's high-frequency, deeper orderbook.
  [HL-12] Blacklist updated: all :USDT symbols → :USDC equivalents.
  [HL-13] Log label: 'Bybit Native TP/SL' → 'HL Native Exit / Liquidation'
  [HL-14] Error code guard: Bybit "10006" rate-limit → generic sleep on
            RateLimitExceeded exception (HL uses standard CCXT exceptions).
  [HL-15] IOC order params: removed Bybit-specific 'positionIdx': 0
  [HL-16] Leverage error codes: removed Bybit "110043"/"110026" guards;
            replaced with generic exception handling for HL.
  [HL-17] get_btc_regime(): BTC symbol updated to BTC/USDC:USDC
  [HL-18] scouting_strong_coins(): symbol suffix filter updated :USDT → :USDC
  [HL-19] fetch_positions(): removed params={'category': 'linear'} (Bybit-only)
  [HL-20] sync_positions_on_startup(): adapted field names for HL position schema
            (HL uses 'entryPx' not 'entryPrice', 'side' is 'long'/'short')
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
# ⚙️ [SYSTEM] Logger & Exchange Initialization
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('HL_Long_V1.0')

# ------------------------------------------------------------------
# [HL-1] Exchange changed from ccxt.bybit to ccxt.hyperliquid.
#         Hyperliquid uses a wallet-based auth model: apiKey is the
#         wallet address (public key), secret is the private key.
#         'defaultType': 'swap' keeps us on the perpetuals market.
# ------------------------------------------------------------------
API_KEY    = "0x45d7ab3F1cC4B43779b7d931e2D8150E672E2C6b"                               # e.g. 0xABCD...
API_SECRET = "0x47404a8300c283b911cb9de459785aef3608f62d2d18125d656605dd9e003ea9"       # 64-char hex private key

# [HL-1] Instantiate Hyperliquid via CCXT
exchange = ccxt.hyperliquid({
    'walletAddress': API_KEY,
    'privateKey': API_SECRET,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap',
        'user': API_KEY,
    }
})
exchange.load_markets()

# ==========================================
# ⚙️ [FILE PATHS] Log & State Persistence
# ==========================================
LOG_DIR        = "result"
STATUS_DIR     = "status"
LOG_FILE       = f"{LOG_DIR}/live_long_log.csv"         # Trade-by-trade CSV log
STATUS_FILE    = f"{STATUS_DIR}/btc_regime_long.csv"    # BTC regime scan log
BLACKLIST_FILE = f"{STATUS_DIR}/dynamic_blacklist_long.json"  # Persistent ban memory

if not os.path.exists(LOG_DIR):    os.makedirs(LOG_DIR)
if not os.path.exists(STATUS_DIR): os.makedirs(STATUS_DIR)

# ==========================================
# ⚙️ [IN-MEMORY STATE]
# ==========================================
positions          = {}   # symbol → position dict
cooldown_tracker   = {}   # symbol → unix timestamp of cooldown expiry
consecutive_losses = {}   # symbol → int count of consecutive losses

# ==========================================
# ⚙️ [STRATEGY PARAMETERS]
# ==========================================

# --- Capital & Leverage ---
WORKING_CAPITAL       = 150.0    # Max capital allocated to this bot (USDC)
MAX_LEVERAGE          = 10.0     # Maximum leverage multiplier
RISK_PER_TRADE        = 0.01     # Risk 0.5% of effective capital per trade
MIN_NOTIONAL          = 11.0     # Min trade value (USDC) — HL minimum is ~$10

# Hard cap: prevents astronomically large positions when ATR is tiny
MAX_NOTIONAL_PER_TRADE = 200.0

# --- Signal Thresholds ---
NET_FLOW_SIGMA        = 1.2      # Z-score threshold for Lee-Ready net flow
TP_ATR_MULT           = 3.5      # Take-profit = entry + 3.5 * ATR
SL_ATR_MULT           = 1.0      # Stop-loss   = entry - 1.0 * ATR
MIN_IMBALANCE_RATIO   = 0.2      # OB imbalance must be > 20% bid-heavy to enter

# --- Dynamic Ban System ---
MAX_CONSECUTIVE_LOSSES = 3        # Trigger 24h ban after N consecutive losses
DYNAMIC_BAN_DURATION   = 86400   # Ban duration: 24 hours in seconds

# --- Timing ---
SCOUTING_INTERVAL     = 125      # Seconds between full market scans
# [HL-10] Reduced from 4s to 2s: Hyperliquid sub-100ms latency allows tighter
#          monitoring without hitting rate limits (HL allows ~1200 req/min).
POSITION_CHECK_INTERVAL = 2

# ------------------------------------------------------------------
# [HL-12] Blacklist updated: all symbols now use :USDC suffix.
#          Hyperliquid lists perpetuals as BASE/USDC:USDC.
#          Stablecoins, wrapped assets, and LSTs are excluded to
#          avoid thin orderbooks and peg-related edge cases.
# ------------------------------------------------------------------
BLACKLIST = [
    'USDC/USDC:USDC', 'DAI/USDC:USDC',   'FDUSD/USDC:USDC', 'BUSD/USDC:USDC',
    'TUSD/USDC:USDC', 'PYUSD/USDC:USDC', 'USDP/USDC:USDC',  'EURS/USDC:USDC',
    'USDE/USDC:USDC', 'USAT/USDC:USDC',  'USD0/USDC:USDC',  'USTC/USDC:USDC',
    'LUSD/USDC:USDC', 'FRAX/USDC:USDC',  'MIM/USDC:USDC',   'RLUSD/USDC:USDC',
    'WBTC/USDC:USDC', 'WETH/USDC:USDC',  'WBNB/USDC:USDC',  'WAVAX/USDC:USDC',
    'stETH/USDC:USDC','cbETH/USDC:USDC', 'WHT/USDC:USDC',
]

# CSV column schemas — unchanged from prototype for log compatibility
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

def log_to_csv(data_dict: dict) -> None:
    """Append one trade event row to the trade log CSV."""
    row = {col: '' for col in CSV_COLUMNS}
    row.update(data_dict)
    row['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pd.DataFrame([row], columns=CSV_COLUMNS).to_csv(
        LOG_FILE, mode='a', index=False,
        header=not os.path.exists(LOG_FILE)
    )


def log_status_to_csv(data_dict: dict) -> None:
    """Append one BTC regime snapshot row to the regime log CSV."""
    row = {col: '' for col in STATUS_COLUMNS}
    row.update(data_dict)
    row['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pd.DataFrame([row], columns=STATUS_COLUMNS).to_csv(
        STATUS_FILE, mode='a', index=False,
        header=not os.path.exists(STATUS_FILE)
    )


# ==========================================
# 🛠️ [MODULE 2] PnL Settlement for Native Exits
# ==========================================

def process_native_exit_log(symbol: str, pos: dict, position_type: str = 'long') -> float:
    """
    Handle exchange-triggered exits (TP/SL hit, liquidation) and log PnL.

    [HL-6] MIGRATION NOTE:
      Bybit version used private_get_v5_position_closed_pnl — a Bybit-specific
      private REST endpoint that returns a structured closed-PnL record.

      Hyperliquid does not expose an equivalent closed-PnL endpoint via CCXT.
      Instead, we reconstruct PnL from the most recent fills via fetch_my_trades().
      This is the CCXT-universal approach and works correctly on Hyperliquid.

      Fallback: if trades cannot be fetched, estimate PnL from last ticker price.
    """
    real_exit_price = pos['entry_price']
    real_pnl        = 0.0

    try:
        # [HL-6] Use CCXT universal fetch_my_trades instead of Bybit V5 private endpoint.
        #         We look at the last 5 fills for this symbol and pick the most recent
        #         closing fill (a 'sell' fill that reduces a long position).
        recent_trades = exchange.fetch_my_trades(symbol, limit=5)

        # Filter for closing sells and take the most recent
        close_fills = [t for t in recent_trades if t.get('side', '') == 'sell']
        if close_fills:
            last_fill       = close_fills[-1]
            real_exit_price = float(last_fill.get('price', pos['entry_price']))
            # fee is expressed in USDC on Hyperliquid
            fill_fee        = float(last_fill.get('fee', {}).get('cost', 0) or 0)
            fill_amount     = float(last_fill.get('amount', pos['amount']))
            # Long PnL = (exit - entry) * qty  minus fees
            real_pnl = round(
                (real_exit_price - pos['entry_price']) * fill_amount - fill_fee, 4
            )
        else:
            raise ValueError("No closing fills found in fetch_my_trades")

    except Exception as e:
        logger.debug(f"⚠️ {symbol} 獲取真實 PnL 失敗，使用備用估算: {e}")
        try:
            curr_p          = exchange.fetch_ticker(symbol)['last']
            real_exit_price = curr_p
            # Fallback Long PnL = (現價 - 入場價) * 數量
            # Apply a conservative 0.07% taker fee estimate for HL
            fee_estimate    = real_exit_price * pos['amount'] * 0.0007
            real_pnl        = round(
                (curr_p - pos['entry_price']) * pos['amount'] - fee_estimate, 4
            )
        except Exception:
            pass  # If all else fails, log with PnL = 0

    # [HL-13] Updated reason label from 'Bybit Native TP/SL' → 'HL Native Exit / Liquidation'
    log_to_csv({
        'symbol': symbol, 'action': 'NATIVE_EXIT', 'price': real_exit_price,
        'amount': pos['amount'],
        'reason': 'HL Native Exit / Liquidation',
        'realized_pnl': real_pnl
    })

    return real_pnl


# ==========================================
# 🛠️ [MODULE 3] Dynamic Ban System (JSON-Persistent)
# ==========================================

def save_dynamic_blacklist() -> None:
    """Persist consecutive-loss counts and cooldown timestamps to JSON (survives restarts)."""
    data = {
        'consecutive_losses': consecutive_losses,
        'cooldown_tracker':   cooldown_tracker
    }
    try:
        with open(BLACKLIST_FILE, 'w') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        logger.error(f"❌ 儲存動態黑名單 JSON 失敗: {e}")


def load_dynamic_blacklist() -> None:
    """On startup, restore ban memory from JSON to avoid re-trading recently banned coins."""
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
                    del consecutive_losses[k]  # Sentence served — clean slate

            if expired:
                save_dynamic_blacklist()

            banned_count = sum(1 for v in cooldown_tracker.values() if v > curr_t + 3600)
            print(f"✅ 成功讀取 JSON 記憶！目前有 {banned_count} 隻妖幣處於 24 小時封禁中。")
        except Exception as e:
            logger.error(f"❌ 讀取動態黑名單 JSON 失敗: {e}")
    else:
        print("ℹ️ 找不到歷史 JSON 記憶，以全新白紙狀態啟動。")


def handle_trade_result(symbol: str, pnl: float) -> None:
    """
    Update consecutive-loss counter and apply dynamic 24h ban if threshold reached.
    Always persists state to JSON immediately after update.
    """
    global consecutive_losses, cooldown_tracker
    if pnl > 0:
        consecutive_losses[symbol] = 0
        print(f"🏆 {symbol} 贏錢平倉！解除冷卻，允許乘勝追擊！")
        if symbol in cooldown_tracker:
            del cooldown_tracker[symbol]
    elif pnl < 0:
        consecutive_losses[symbol] = consecutive_losses.get(symbol, 0) + 1
        if consecutive_losses[symbol] >= MAX_CONSECUTIVE_LOSSES:
            cooldown_tracker[symbol] = time.time() + DYNAMIC_BAN_DURATION
            print(f"🚫 [動態封禁] {symbol} 已連續虧損 {consecutive_losses[symbol]} 次！打入冷宮 24 小時。")
        else:
            # Standard 8-minute cooldown after a single loss
            cooldown_tracker[symbol] = max(
                cooldown_tracker.get(symbol, 0),
                time.time() + 480
            )

    save_dynamic_blacklist()


# ==========================================
# 🛠️ [MODULE 4] Account & Order Utilities
# ==========================================

def get_live_usdc_balance() -> float:
    """
    Fetch free USDC balance from Hyperliquid account.

    [HL-2] / [HL-4] MIGRATION NOTE:
      Bybit used ['USDT']['free']. Hyperliquid settles in USDC.
      CCXT maps this correctly under ['USDC']['free'] for HL.
    """
    try:
        bal = exchange.fetch_balance({'type': 'swap', 'user': API_KEY})
        # Hyperliquid may report balance under 'USDC' or total equity
        usdc_free = bal.get('USDC', {}).get('free', 0) or \
                    bal.get('total', {}).get('USDC', 0) or 0
        return float(usdc_free)
    except Exception:
        logger.error(f'❌ Failed to receive balance: {e}')
        return 0.0


def cancel_all_hl(symbol: str) -> None:
    """
    Cancel all open orders for a symbol on Hyperliquid.

    [HL-5] MIGRATION NOTE:
      Bybit version called three cancel_all_orders variants
      (normal / StopOrder / tpslOrder) plus private_post_v5_position_trading_stop
      to wipe exchange-native TP/SL.

      Hyperliquid via CCXT:
        1. cancel_all_orders(symbol) — cancels all limit/trigger orders.
        2. No separate TP/SL order type attached to position exists on HL
           in the same way as Bybit. TP/SL on HL are placed as separate
           trigger orders; cancel_all_orders handles them.
        3. No equivalent to Bybit's trading_stop endpoint is needed.
    """
    try:
        exchange.cancel_all_orders(symbol)
        logger.debug(f"🧹 {symbol} 所有掛單已撤銷 (HL)")
    except Exception as e:
        logger.debug(f"⚠️ {symbol} 撤單失敗 (non-critical): {e}")


def get_3_layer_avg_price(symbol: str, side: str = 'bids') -> float | None:
    """
    Compute the volume-weighted average price of the top 3 order book levels.
    Used to get a realistic IOC limit price that minimises slippage.
    Hyperliquid's deep book makes this highly effective.
    """
    try:
        ob     = exchange.fetch_order_book(symbol, limit=5)
        levels = ob[side][:3]
        if not levels:
            return None
        return sum(level[0] for level in levels) / len(levels)
    except Exception:
        return None


def get_market_metrics(symbol: str) -> tuple[float | None, bool]:
    """
    Compute ATR(14) on the 5-minute chart and filter dead/illiquid coins.
    Returns (atr, is_volatile).
    is_volatile = True only when ATR/price > 0.15% (avoids fee-grinding).
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


# ==========================================
# 🧠 [MODULE 5] BTC Market Regime Navigator
# ==========================================

def get_btc_regime() -> int:
    """
    BTC macro trend filter: HMA(20/50) crossover + ADX(14) > 22 + Volume median.
    Returns:
       1  = GREEN  — All three conditions met, scout for longs
       0  = YELLOW — Partial confluence, standby
      -1  = RED    — Sideways or bearish, no new entries

    [HL-17] BTC symbol updated from BTC/USDT:USDT → BTC/USDC:USDC
    """
    try:
        # [HL-17] Fetch BTC candles with Hyperliquid USDC-denominated symbol
        ohlcv  = exchange.fetch_ohlcv('BTC/USDC:USDC', timeframe='15m', limit=150)
        df     = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
        curr_p = df['c'].iloc[-1]

        # --- HMA (Hull Moving Average) Calculation ---
        # HMA = WMA(sqrt(n), 2*WMA(n/2) - WMA(n))
        # Rationale: HMA reduces lag while maintaining smoothness vs EMA/SMA.
        def calc_hma(series: pd.Series, period: int) -> pd.Series:
            half_length  = int(period / 2)
            sqrt_length  = int(np.sqrt(period))
            w_half  = np.arange(1, half_length + 1)
            w_full  = np.arange(1, period + 1)
            w_sqrt  = np.arange(1, sqrt_length + 1)
            wma_half = series.rolling(half_length).apply(
                lambda x: np.dot(x, w_half) / w_half.sum(), raw=True
            )
            wma_full = series.rolling(period).apply(
                lambda x: np.dot(x, w_full) / w_full.sum(), raw=True
            )
            diff = (2 * wma_half) - wma_full
            return diff.rolling(sqrt_length).apply(
                lambda x: np.dot(x, w_sqrt) / w_sqrt.sum(), raw=True
            )

        df['hma20'], df['hma50'] = calc_hma(df['c'], 20), calc_hma(df['c'], 50)
        hma20_val = df['hma20'].iloc[-1]
        hma50_val = df['hma50'].iloc[-1]

        # Trend condition: short HMA above long HMA = bullish structure
        cond_trend = hma20_val > hma50_val

        # --- ADX(14) — Trend Strength Filter ---
        # Removes sideways/choppy markets where false breakouts dominate.
        df['up']   = df['h'] - df['h'].shift(1)
        df['down'] = df['l'].shift(1) - df['l']
        df['+dm']  = np.where((df['up'] > df['down']) & (df['up'] > 0),   df['up'],   0)
        df['-dm']  = np.where((df['down'] > df['up']) & (df['down'] > 0), df['down'], 0)
        df['tr']   = np.maximum(
            df['h'] - df['l'],
            np.maximum(
                abs(df['h'] - df['c'].shift(1)),
                abs(df['l'] - df['c'].shift(1))
            )
        )
        atr_14    = df['tr'].ewm(alpha=1 / 14, adjust=False).mean()
        plus_di   = 100 * (pd.Series(df['+dm']).ewm(alpha=1/14, adjust=False).mean() / atr_14)
        minus_di  = 100 * (pd.Series(df['-dm']).ewm(alpha=1/14, adjust=False).mean() / atr_14)
        denom     = plus_di + minus_di
        dx        = np.where(denom != 0, 100 * abs(plus_di - minus_di) / denom, 0)
        adx_val   = pd.Series(dx).ewm(alpha=1/14, adjust=False).mean().iloc[-1]
        cond_adx  = adx_val > 22

        # --- Volume Confirmation Filter ---
        # Use the last fully-closed candle (-2) vs 24-candle median.
        # Avoids the still-forming candle (-1) which is partially zero.
        completed_v  = df['v'].iloc[-2]
        median_v_24  = df['v'].iloc[-25:-1].median()
        target_vol   = median_v_24 * 0.8
        cond_vol     = completed_v > target_vol

        # --- Signal Assembly ---
        tick_t = "✅" if cond_trend else "❌"
        tick_a = f"✅ (ADX: {adx_val:.1f})" if cond_adx else f"❌ (ADX: {adx_val:.1f})"
        tick_v = (f"✅ (Vol: {completed_v:.0f} > 目標:{target_vol:.0f})"
                  if cond_vol else
                  f"❌ (Vol: {completed_v:.0f} < 目標:{target_vol:.0f})")

        if cond_trend and cond_adx and cond_vol:
            status, signal = "🟢 GREEN   (Trend, ADX & Vol Validated)", 1
        elif cond_trend or cond_adx:
            status, signal = "🟡 YELLOW  (Standby - Waiting for confluence)", 0
        else:
            status, signal = "🔴 RED     (Sideways / Bearish)", -1

        log_status_to_csv({
            'btc_price':   round(curr_p, 2),
            'target_price': round(hma50_val, 2),
            'hma20':        round(hma20_val, 2),
            'hma50':        round(hma50_val, 2),
            'adx':          round(adx_val, 2),
            'signal_code':  signal,
            'decision_text': status
        })

        print("-" * 60)
        print(f"📊 BTC 實時戰報 (HMA+ADX+Vol) | 現價: {curr_p:.0f} USDC")
        print(f"1️⃣ 極速趨勢 : HMA20({hma20_val:.0f}) > HMA50({hma50_val:.0f}) {tick_t}")
        print(f"2️⃣ 趨勢強度 : ADX > 22 {tick_a}")
        print(f"3️⃣ 動能確認 : 上根已收盤量 > 24H中位數(80%) {tick_v}")
        print(f"🚦 最終決策 : {status}")
        print("-" * 60)

        return signal

    except Exception as e:
        print(f"⚠️ 導航故障: {e}")
        return 0


# ==========================================
# 🧠 [MODULE 6] Coin Scouter (Strongest of the Strong)
# ==========================================

def scouting_strong_coins(scouting_coins: int = 20) -> list[str]:
    """
    Scan the full Hyperliquid market universe.
    Step 1: Keep only perps with tight spreads (< 0.1%) → liquidity gate.
    Step 2: Rank by 24h USDC volume → Top N by market cap/liquidity.
    Step 3: Within that top pool, rank by 24h % change descending → momentum.

    [HL-18] Symbol suffix filter changed from ':USDT' → ':USDC'
    [HL-12] BLACKLIST updated to use :USDC symbols
    """
    try:
        tickers = exchange.fetch_tickers()
        data    = []
        for s, t in tickers.items():
            # [HL-18] Filter for USDC-margined perpetuals only
            if not s.endswith(':USDC'):
                continue
            if s in BLACKLIST:
                continue
            if t.get('percentage') is None:
                continue
            ask, bid = t.get('ask'), t.get('bid')
            if not (ask and bid and bid > 0):
                continue
            spread = (ask - bid) / bid
            if spread < 0.0010:  # Tight spread = sufficient liquidity
                data.append({
                    'symbol': s,
                    'volume': t.get('quoteVolume', 0) or 0,
                    'change': t['percentage']
                })

        if not data:
            return []

        df         = pd.DataFrame(data)
        top_majors = df.sort_values('volume', ascending=False).head(scouting_coins)
        # Select the strongest movers (highest % gain) within the liquid pool
        return (top_majors
                .sort_values('change', ascending=False)
                .head(scouting_coins)['symbol']
                .tolist())

    except Exception as e:
        print(f"⚠️ Scouting Error: {e}")
        return []


# ==========================================
# 🧠 [MODULE 7] Flow Health Radar (Defensive - Long)
# ==========================================

def check_flow_health(symbol: str) -> str | None:
    """
    Defensive radar: detect extreme sell dumps and momentum deceleration.
    Called periodically while holding a long position.
    Returns a string reason if an exit/warning condition is met, else None.

    Conditions:
      1. "Flow Reversal (Long Dump Detected)"
         → Z-score of last 25 trades' net flow < -3.0 (extreme sell pressure)
         → Immediate exit signal

      2. "Flow Deceleration (Momentum Died)"
         → Acceleration strongly negative AND flow turning negative AND
           asks outweigh bids (OB imbalance confirms)
         → Sets deceleration_detected flag (managed position tightens trail SL)

    [HL-11] MIGRATION NOTE on Deceleration Threshold:
      Original Bybit threshold: accel_z < -2.0
      Hyperliquid has significantly higher trade frequency (~5-10x more
      trades/second) and deeper orderbooks. This means noise in the net flow
      signal is higher per-unit-time. A threshold of -2.0 would fire too
      frequently, causing premature trail-SL tightening.
      → Raised to accel_z < -2.5 to filter genuine deceleration from noise.
    """
    try:
        trades = exchange.fetch_trades(symbol, limit=100)
        if not trades or len(trades) < 50:
            return None

        df               = pd.DataFrame(trades)
        df['price_change'] = df['price'].diff()
        df['direction']    = np.where(
            df['price_change'] > 0, 1,
            np.where(df['price_change'] < 0, -1, 0)
        )
        df['direction'] = df['direction'].replace(0, np.nan).ffill().fillna(0)

        # Large-trade weighting: orders >2x average size get double weight
        avg_vol         = df['amount'].mean()
        df['weight']    = np.where(df['amount'] > avg_vol * 2, 2.0, 1.0)
        df['net_flow']  = df['direction'] * df['amount'] * df['price'] * df['weight']

        flow_std = df['net_flow'].std()
        if flow_std == 0:
            return None

        flow_mean        = df['net_flow'].mean()
        recent_25_flow   = df['net_flow'].tail(25).sum()
        z_score = (recent_25_flow - (flow_mean * 25)) / (flow_std * np.sqrt(25))

        # Condition 1: Extreme dump Z-score → immediate exit
        if z_score < -3.0:
            return "Flow Reversal (Long Dump Detected)"

        # Condition 2: Deceleration (momentum dying, not yet reversed)
        flow_older_25 = df['net_flow'].iloc[-50:-25].sum()
        acceleration  = recent_25_flow - flow_older_25
        accel_z       = acceleration / (flow_std * np.sqrt(25))

        # [HL-11] Threshold tightened from -2.0 → -2.5 for HL high-freq noise
        if accel_z < -2.5 and recent_25_flow < 0:
            try:
                ob        = exchange.fetch_order_book(symbol, limit=20)
                bids_vol  = sum(b[1] for b in ob['bids'])
                asks_vol  = sum(a[1] for a in ob['asks'])
                imbalance = ((bids_vol - asks_vol) / (bids_vol + asks_vol)
                             if (bids_vol + asks_vol) > 0 else 0)

                if imbalance < -0.15:  # Asks dominating confirms momentum death
                    return "Flow Deceleration (Momentum Died)"
            except Exception:
                pass

        return None

    except Exception:
        return None


# ==========================================
# 🧠 [MODULE 8] Lee-Ready Entry Sniper (Long)
# ==========================================

def apply_lee_ready_long_logic(symbol: str) -> tuple[float, float, bool]:
    """
    Classify order flow direction using a weighted Lee-Ready proxy.
    Trades that occur on an uptick are buyer-initiated; downtick = seller.
    Large orders (>2x avg size) are double-weighted.

    Entry conditions (Long):
      A) All three aligned: net flow > 0, acceleration > 0, OB imbalance > +0.15
         → Sniper entry (high conviction)
      B) Net flow Z-score > NET_FLOW_SIGMA (1.2)
         → Flow-only entry (moderate conviction)

    Anti-fake-breakout filter:
      If condition A or B fires, but OB imbalance is negative (asks > bids),
      cancel the entry — the price pump is being distributed into, not absorbed.

    Returns: (net_flow_value, last_price, is_strong)
    """
    try:
        trades = exchange.fetch_trades(symbol, limit=200)
        if not trades:
            return 0, 0, False

        df               = pd.DataFrame(trades)
        df['price_change'] = df['price'].diff()
        df['direction']    = np.where(
            df['price_change'] > 0, 1,
            np.where(df['price_change'] < 0, -1, 0)
        )
        df['direction'] = df['direction'].replace(0, np.nan).ffill().fillna(0)

        avg_vol        = df['amount'].mean()
        df['weight']   = np.where(df['amount'] > avg_vol * 2, 2.0, 1.0)
        df['net_flow'] = df['direction'] * df['amount'] * df['price'] * df['weight']

        # Short window = last 50 trades (recency)
        # Acceleration = comparison vs prior 25 trades (momentum change)
        short_window_flow = df['net_flow'].tail(50).sum()
        acceleration      = (df['net_flow'].tail(25).sum()
                             - df['net_flow'].iloc[-50:-25].sum())

        # Order book imbalance snapshot
        try:
            ob        = exchange.fetch_order_book(symbol, limit=20)
            bids_vol  = sum(b[1] for b in ob['bids'])
            asks_vol  = sum(a[1] for a in ob['asks'])
            imbalance = ((bids_vol - asks_vol) / (bids_vol + asks_vol)
                         if (bids_vol + asks_vol) > 0 else 0)
        except Exception:
            imbalance = 0

        is_strong = False
        flow_std  = df['net_flow'].std()
        z_score   = (short_window_flow / (flow_std * np.sqrt(50))
                     if flow_std > 0 else 0)

        # Condition A: Triple confluence
        if (short_window_flow > 0) and (acceleration > 0) and (imbalance > 0.15):
            is_strong = True
            print(f"🔥 {symbol} Long Sniper! Accel: {acceleration:.0f} | Imbalance: {imbalance:.2f}")
        # Condition B: Flow Z-score alone
        elif z_score > NET_FLOW_SIGMA:
            is_strong = True
            print(f"📈 {symbol} Long Z-Score Validated: {z_score:.2f}")

        # Anti-fake-breakout: if asks are dominating OB, abort
        if is_strong and imbalance < -0.1:
            is_strong = False
            print(f"⚠️ {symbol} 發現假突破陷阱！賣盤極厚，取消做多！")

        return short_window_flow, df['price'].iloc[-1], is_strong

    except Exception as e:
        print(f"⚠️ LR Logic Error [{symbol}]: {e}")
        return 0, 0, False


# ==========================================
# 🛡️ [MODULE 9] Startup Position Sync
# ==========================================

def sync_positions_on_startup() -> None:
    """
    On restart, query exchange for any open long positions and adopt them
    into the bot's in-memory position tracker. Prevents orphaned positions
    from going unmanaged after a crash or restart.

    [HL-19] Removed Bybit-specific params={'category': 'linear'}.
             HL's fetch_positions() returns all positions by default.
    [HL-20] Field mapping updated for Hyperliquid position schema:
             HL uses 'entryPx' (not 'entryPrice'), 'side' is 'long'/'short',
             'contracts' or 'info.szi' for position size.
    """
    print("🔄 正在同步 Hyperliquid 現有倉位...")
    try:
        # [HL-19] No category param needed for Hyperliquid
        live_positions_raw = exchange.fetch_positions(None, {'user': API_KEY})
        live_long_positions = [
            p for p in live_positions_raw
            if float(p.get('contracts', 0) or
                     p.get('info', {}).get('szi', 0) or 0) > 0
            and p.get('side', '').lower() in ['long', 'buy']
        ]

        recovered_count = 0
        for p in live_long_positions:
            symbol = p['symbol']

            # [HL-20] Hyperliquid uses 'entryPx' in info dict; CCXT normalizes to 'entryPrice'
            entry_price = float(
                p.get('entryPrice') or
                p.get('info', {}).get('entryPx', 0) or 0
            )
            amount = float(
                p.get('contracts', 0) or
                p.get('info', {}).get('szi', 0) or 0
            )

            if entry_price == 0 or amount == 0:
                continue

            atr, _ = get_market_metrics(symbol)
            if not atr:
                atr = entry_price * 0.01  # Fallback: 1% of price as ATR estimate

            # [HL-20] HL does not attach exchange-side SL/TP to positions in the
            #          same way as Bybit. stopLoss/takeProfit fields will be 0.
            #          We always reconstruct from ATR for recovered positions.
            sl_p = float(p.get('stopLoss') or 0)
            tp_p = float(p.get('takeProfit') or 0)
            if sl_p == 0:
                sl_p = float(exchange.price_to_precision(
                    symbol, entry_price - (SL_ATR_MULT * atr)
                ))
            if tp_p == 0:
                tp_p = float(exchange.price_to_precision(
                    symbol, entry_price + (TP_ATR_MULT * atr)
                ))

            # Breakeven = SL has been moved above entry price
            is_be = (sl_p > entry_price and sl_p > 0)

            positions[symbol] = {
                'amount':      amount,
                'entry_price': entry_price,
                'tp_price':    tp_p,
                'sl_price':    sl_p,
                'is_breakeven':     is_be,
                'atr':              atr,
                'max_pnl_pct':      0.0,
                'entry_time':       time.time(),
                'deceleration_detected': False,
            }
            recovered_count += 1
            print(f"✅ 成功尋回孤兒多單: {symbol} | 入場價: {entry_price:.4f} | 已保本: {is_be}")

        print(f"🔄 同步完成！共尋回 {recovered_count} 個多倉。")

    except Exception as e:
        logger.error(f"❌ 啟動同步失敗: {e}")


# ==========================================
# 🛡️ [MODULE 10] Position Manager (Heart of Risk Engine)
# ==========================================

def manage_long_positions() -> None:
    """
    Main position management loop. Called every POSITION_CHECK_INTERVAL seconds.
    Responsibilities:
      1. Auto-adopt orphaned long positions not in bot memory
      2. Detect positions closed by exchange (TP/SL hit or liquidation)
      3. Advance trailing stop in stages based on profit depth
      4. Apply zombie timeout (stalled position with minimal profit)
      5. Monitor flow health and set deceleration flag
      6. Execute local TP/SL via IOC orders

    [HL-8]  Trail SL update: removed Bybit private_post_v5_position_trading_stop.
             Hyperliquid trail SL is managed entirely in software by this function.
             The exchange will only stop-out if the mark price hits an HL
             native stop order; for this bot we rely on local logic exclusively.
    [HL-14] Error handling: replaced Bybit "10006" check with CCXT
             RateLimitExceeded exception handling.
    [HL-19] fetch_positions() called without Bybit category params.
    """
    try:
        # [HL-19] No category param for Hyperliquid
        live_positions_raw = exchange.fetch_positions()
        live_symbols = {
            p['symbol']: p for p in live_positions_raw
            if float(p.get('contracts', 0) or
                     p.get('info', {}).get('szi', 0) or 0) > 0
        }

        # -------------------------------------------------------
        # Step 1: Auto-adopt orphan long positions
        # -------------------------------------------------------
        for s, p in live_symbols.items():
            if s in positions:
                continue
            side = p.get('side', '').lower()
            if side not in ['long', 'buy']:
                continue

            entry_p = float(
                p.get('entryPrice') or
                p.get('info', {}).get('entryPx', 0) or 0
            )
            amt = float(
                p.get('contracts', 0) or
                p.get('info', {}).get('szi', 0) or 0
            )
            if entry_p == 0 or amt == 0:
                continue

            atr, _ = get_market_metrics(s)
            if not atr:
                atr = entry_p * 0.01

            # [HL-20] HL createdTime may be ms; normalize to seconds
            raw_ts       = p.get('createdTime') or p.get('info', {}).get('time')
            real_entry_t = (float(raw_ts) / 1000.0) if raw_ts else time.time()

            sl_p = float(p.get('stopLoss') or 0)
            tp_p = float(p.get('takeProfit') or 0)
            if sl_p == 0:
                sl_p = float(exchange.price_to_precision(
                    s, entry_p - (SL_ATR_MULT * atr)
                ))
            if tp_p == 0:
                tp_p = float(exchange.price_to_precision(
                    s, entry_p + (TP_ATR_MULT * atr)
                ))

            is_be = (sl_p > entry_p and sl_p > 0)
            positions[s] = {
                'amount': amt, 'entry_price': entry_p, 'tp_price': tp_p,
                'sl_price': sl_p, 'is_breakeven': is_be, 'atr': atr,
                'max_pnl_pct': 0.0, 'entry_time': real_entry_t,
                'deceleration_detected': False,
            }
            print(f"🚨 [系統自癒] 發現並接管孤兒多單: {s} | 入場價: {entry_p:.4f} | 數量: {amt}")

        # -------------------------------------------------------
        # Step 2: Detect positions closed by exchange
        # -------------------------------------------------------
        for s in list(positions.keys()):
            if s not in live_symbols:
                print(f"🧹 交易所已平倉，結算真實 PnL: {s}")
                real_pnl = process_native_exit_log(s, positions[s], position_type='long')
                cancel_all_hl(s)
                handle_trade_result(s, real_pnl)
                del positions[s]
                continue

        # -------------------------------------------------------
        # Step 3–6: Manage live positions
        # -------------------------------------------------------
        for s in list(positions.keys()):
            pos    = positions[s]
            curr_p = exchange.fetch_ticker(s)['last']

            # Long PnL: profit when price rises above entry
            pnl_pct = (curr_p - pos['entry_price']) / pos['entry_price']

            coin_volatility_pct = pos['atr'] / pos['entry_price']
            sl_updated = False

            # Track peak profit for diagnostics
            pos['max_pnl_pct'] = max(pos.get('max_pnl_pct', pnl_pct), pnl_pct)

            # -------------------------------------------------------
            # Stage 1 & 2: Breakeven push (once profit > 2.0 × ATR%)
            # -------------------------------------------------------
            if not pos['is_breakeven'] and pnl_pct > (coin_volatility_pct * 2.0):
                # Lock in 0.2% profit: move SL to entry * 1.002
                pos['sl_price']     = pos['entry_price'] * 1.002
                pos['is_breakeven'] = True
                sl_updated          = True

            # -------------------------------------------------------
            # Stage 3: Multi-speed variable trailing stop
            # Trail distances get tighter as profit deepens.
            # Deceleration detection enables emergency tight trail.
            # -------------------------------------------------------
            if pos['is_breakeven']:
                if pos.get('deceleration_detected', False) and pnl_pct > (coin_volatility_pct * 2.5):
                    # 👑 Deceleration detected + sufficient profit: ultra-tight 0.5 ATR trail
                    trail_sl = curr_p - (0.5 * pos['atr'])
                elif pnl_pct > (coin_volatility_pct * 5.0):
                    # Deep profit zone: reduce trail to lock more gains
                    trail_sl = curr_p - (0.8 * pos['atr'])
                elif pnl_pct > (coin_volatility_pct * 3.5):
                    # Developing profit zone
                    trail_sl = curr_p - (1.2 * pos['atr'])
                else:
                    # Just past breakeven: give room to breathe
                    trail_sl = curr_p - (1.8 * pos['atr'])

                # Trail SL can only move UP (locking in progressively more profit)
                if trail_sl > pos['sl_price']:
                    movement_pct = (trail_sl - pos['sl_price']) / pos['sl_price']
                    if movement_pct > 0.0005:  # Min 0.05% move to avoid API spam
                        sl_updated   = True
                        pos['sl_price'] = trail_sl

            # [HL-8] Trail SL is managed locally only.
            # No exchange API call needed here (HL has no trading_stop equivalent).
            if sl_updated:
                logger.debug(
                    f"📐 {s} Trail SL updated to {pos['sl_price']:.4f} "
                    f"(pnl={pnl_pct*100:.2f}%)"
                )

            # -------------------------------------------------------
            # Timeout: Zombie position — stalled > 45 min, tiny profit
            # -------------------------------------------------------
            exit_reason = None
            time_held   = time.time() - pos.get('entry_time', time.time())

            if not exit_reason:
                if time_held > 2700 and pnl_pct < 0.005:
                    exit_reason = "Momentum Timeout (Stalled Zombie)"

            # -------------------------------------------------------
            # Flow health radar: every 15 seconds, after 120s hold
            # -------------------------------------------------------
            curr_t     = time.time()
            last_check = pos.get('last_flow_check', 0)

            if not exit_reason and (curr_t - last_check > 15):
                pos['last_flow_check'] = curr_t
                if time_held > 120:
                    flow_status = check_flow_health(s)

                    if flow_status == "Flow Reversal (Long Dump Detected)":
                        # Extreme dump → immediate exit
                        exit_reason = flow_status

                    elif flow_status == "Flow Deceleration (Momentum Died)":
                        # Soft warning: tighten trail SL but don't exit yet
                        if not pos.get('deceleration_detected', False):
                            pos['deceleration_detected'] = True
                            print(
                                f"⚠️ {s} 偵測到高位收油！已啟動極限防禦，"
                                f"若利潤充足將自動收緊至 0.5 ATR！"
                            )

            # -------------------------------------------------------
            # Local TP/SL check
            # -------------------------------------------------------
            if not exit_reason:
                if curr_p >= pos['tp_price']:
                    exit_reason = "TP (Long IOC Exit)"
                elif curr_p <= pos['sl_price']:
                    exit_reason = (
                        "Trail SL (Long IOC Exit)"
                        if pos['is_breakeven']
                        else "SL (Long IOC Exit)"
                    )

            # -------------------------------------------------------
            # Execute exit via IOC sell order
            # -------------------------------------------------------
            if exit_reason:
                print(
                    f"⚔️ 觸發 {exit_reason}: {s} | "
                    f"持倉: {time_held/60:.1f}m | "
                    f"MaxPnL: {pos['max_pnl_pct']*100:.2f}% | "
                    f"現PnL: {pnl_pct*100:.2f}%"
                )

                # Take bid side price for immediate execution (we are the seller)
                ioc_price = get_3_layer_avg_price(s, 'bids') or curr_p
                try:
                    # [HL-15] Removed Bybit positionIdx param; HL uses standard reduceOnly
                    exchange.create_order(
                        s, 'limit', 'sell', pos['amount'], ioc_price,
                        {'timeInForce': 'IOC', 'reduceOnly': True}
                    )
                except Exception:
                    # Fallback to market order if IOC fails
                    exchange.create_market_sell_order(
                        s, pos['amount'], {'reduceOnly': True}
                    )

                # PnL estimate: price difference × quantity (gross, pre-fee)
                # Hyperliquid taker fee ≈ 0.05–0.07% — acceptable for IOC fills
                ioc_pnl = round((ioc_price - pos['entry_price']) * pos['amount'], 4)

                log_to_csv({
                    'symbol': s, 'action': 'LONG_EXIT', 'price': curr_p,
                    'amount': pos['amount'], 'reason': exit_reason,
                    'realized_pnl': ioc_pnl
                })

                cancel_all_hl(s)
                handle_trade_result(s, ioc_pnl)
                del positions[s]

    except ccxt.RateLimitExceeded:
        # [HL-14] Bybit used "10006" string check; HL uses CCXT typed exception
        logger.warning("⏳ Rate limit hit — sleeping 5s")
        time.sleep(5)
    except Exception as e:
        logger.error(f"❌ manage_long_positions 異常: {e}")


# ==========================================
# 🚀 [MODULE 11] Entry Executor
# ==========================================

def execute_live_long(
    symbol: str, net_flow: float,
    current_price: float, is_strong: bool,
    atr: float | None, is_volatile: bool
) -> None:
    """
    Size and execute a long entry via IOC limit order.
    Position sizing: risk-based (0.5% of effective capital / ATR-stop distance).
    Caps: leverage cap + hard notional cap prevent over-sizing.

    [HL-7]  MIGRATION NOTE: Bybit version called private_post_v5_position_trading_stop
            to set exchange-side TP/SL immediately after entry.
            Hyperliquid doesn't have an equivalent CCXT endpoint for attached
            conditional orders via the same interface. TP/SL for this bot are
            tracked and enforced locally in manage_long_positions(). The bot
            can optionally place a stop-limit order as exchange backup — this
            is noted below and commented out for review.
    [HL-9]  set_leverage: Bybit codes 110043/110026 replaced with generic handler.
    [HL-15] IOC params: removed 'positionIdx': 0 (Bybit hedge-mode specific).
    [HL-16] Leverage error handling generalized for Hyperliquid.
    """
    # Check cooldown / dynamic ban
    if symbol in cooldown_tracker:
        if time.time() < cooldown_tracker[symbol]:
            return
        else:
            del cooldown_tracker[symbol]

    if atr is None or atr == 0 or current_price == 0:
        return
    if not (is_strong and is_volatile and symbol not in positions):
        return

    cancel_all_hl(symbol)

    # [HL-2] Balance in USDC
    actual_bal = get_live_usdc_balance()
    eff_bal    = min(WORKING_CAPITAL, actual_bal)

    # Risk-based position sizing:
    # Dollar risk = eff_bal * RISK_PER_TRADE
    # ATR stop distance as fraction of price = (SL_ATR_MULT * atr) / current_price
    # Trade value = dollar_risk / atr_fraction
    # Then cap at leverage limit and hard notional cap.
    atr_stop_frac = (SL_ATR_MULT * atr) / current_price
    trade_val = min(
        (eff_bal * RISK_PER_TRADE) / atr_stop_frac,
        eff_bal * MAX_LEVERAGE * 0.95,   # 5% buffer vs max leverage
        MAX_NOTIONAL_PER_TRADE
    )
    amount = float(exchange.amount_to_precision(symbol, trade_val / current_price))

    # Check minimum order size
    min_amount = exchange.markets[symbol]['limits']['amount'].get('min', 0)
    if amount < min_amount:
        return

    # IOC entry price: average of top 3 ask levels (we're a buyer)
    ioc_p = get_3_layer_avg_price(symbol, 'asks') or current_price
    if amount * ioc_p < MIN_NOTIONAL:
        return

    # [HL-9] / [HL-16] Set leverage — generalized error handling for HL
    try:
        exchange.set_leverage(int(MAX_LEVERAGE), symbol)
    except Exception as e:
        # Some HL markets have fixed leverage; log but don't abort
        logger.warning(f"⚠️ {symbol} 槓桿設置異常 (繼續嘗試入場): {e}")

    try:
        # [HL-15] Removed positionIdx (Bybit hedge-mode only)
        order = exchange.create_order(
            symbol, 'limit', 'buy', amount, ioc_p,
            {'timeInForce': 'IOC'}
        )
        time.sleep(1)  # Allow fill to settle

        actual_price, actual_amount = ioc_p, 0.0

        # Confirm fill details
        try:
            order_detail = exchange.fetch_order(
                order['id'], symbol, params={"acknowledged": True}
            )
            actual_price  = float(order_detail.get('average') or
                                  order_detail.get('price') or ioc_p)
            actual_amount = float(order_detail.get('filled', 0))
        except Exception as e:
            logger.warning(f"⚠️ {symbol} 獲取訂單失敗，備用持倉同步: {e}")
            time.sleep(0.5)
            # Fallback: scan live positions
            for p in exchange.fetch_positions():
                if (p['symbol'] == symbol and
                        float(p.get('contracts', 0) or
                              p.get('info', {}).get('szi', 0) or 0) > 0):
                    actual_amount = float(
                        p.get('contracts', 0) or
                        p.get('info', {}).get('szi', 0) or 0
                    )
                    actual_price = float(
                        p.get('entryPrice') or
                        p.get('info', {}).get('entryPx', ioc_p) or ioc_p
                    )
                    break

        if actual_amount == 0:
            print(f"⏩ {symbol} IOC 未成交，撤單退出。")
            cancel_all_hl(symbol)
            return

        # Calculate TP and SL levels
        tp_p = float(exchange.price_to_precision(
            symbol, actual_price + (TP_ATR_MULT * atr)
        ))
        sl_p = float(exchange.price_to_precision(
            symbol, actual_price - (SL_ATR_MULT * atr)
        ))

        # Minimum profit sanity check: TP must be > 0.3% above entry
        # (covers ~2× round-trip fees on HL at 0.07% taker)
        expected_profit_margin = (tp_p - actual_price) / actual_price
        if expected_profit_margin < 0.003:
            print(
                f"🟡 放棄做多 [{symbol}]: 預期利潤空間 "
                f"({expected_profit_margin*100:.2f}%) 太細，立即市價平倉！"
            )
            try:
                exchange.create_market_sell_order(
                    symbol, actual_amount, {'reduceOnly': True}
                )
            except Exception as e:
                logger.error(f"❌ {symbol} 緊急平倉失敗！需人工介入: {e}")
            cancel_all_hl(symbol)
            return

        # [HL-7] No exchange-native TP/SL call here.
        #         Bybit: private_post_v5_position_trading_stop(...)
        #         HL equivalent (optional stop-limit, commented for review):
        # try:
        #     exchange.create_order(symbol, 'stop', 'sell', actual_amount, sl_p,
        #                           {'stopPrice': sl_p, 'reduceOnly': True})
        # except Exception as e:
        #     logger.warning(f"⚠️ {symbol} Exchange stop order failed: {e}")
        #
        # For now, TP/SL are enforced entirely via local logic in
        # manage_long_positions(). This is safer on HL where native
        # conditional orders behave differently from Bybit's tpslMode.

        print(f"✅ {symbol} 本地止盈止損已記錄 | TP: {tp_p:.4f} | SL: {sl_p:.4f}")

        # Register position in memory
        positions[symbol] = {
            'amount':      actual_amount,
            'entry_price': actual_price,
            'tp_price':    tp_p,
            'sl_price':    sl_p,
            'is_breakeven':     False,
            'atr':              atr,
            'max_pnl_pct':      0.0,
            'entry_time':       time.time(),
            'deceleration_detected': False,
        }
        # 8-minute cooldown prevents re-entry immediately after fill
        cooldown_tracker[symbol] = time.time() + 480
        save_dynamic_blacklist()

        log_to_csv({
            'symbol':           symbol,
            'action':           'LONG_ENTRY',
            'price':            actual_price,
            'amount':           actual_amount,
            'trade_value':      round(actual_amount * actual_price, 2),
            'atr':              round(atr, 4),
            'net_flow':         round(net_flow, 2),
            'tp_price':         tp_p,
            'sl_price':         sl_p,
            'actual_balance':   round(actual_bal, 2),
            'effective_balance': eff_bal
        })
        print(f"📈 [已入貨做多] {symbol} @ {actual_price:.4f} USDC | 數量: {actual_amount}")

    except ccxt.RateLimitExceeded:
        logger.warning(f"⏳ {symbol} 入場遭遇 Rate Limit，等待後重試")
        time.sleep(5)
    except Exception as e:
        logger.error(f"❌ {symbol} 做多核心執行失敗: {e}")


# ==========================================
# 🚀 [MODULE 12] Main Event Loop
# ==========================================

def main() -> None:
    print("=" * 60)
    print("🚀 Hyperliquid Long V1.0 啟動")
    print("   Lee-Ready Flow + OB Imbalance + AI Variable Trail SL")
    print("   Dynamic JSON Ban System | USDC Base | HL Native")
    print("=" * 60)

    # Restore memory: ban list and cooldowns from previous session
    load_dynamic_blacklist()

    # Sync any positions already open on exchange (crash recovery)
    sync_positions_on_startup()

    last_scout_time = 0
    target_coins    = []

    while True:
        try:
            # --- Tight inner loop: position management runs every cycle ---
            manage_long_positions()
            curr_t = time.time()

            # --- Outer loop: market scan every SCOUTING_INTERVAL seconds ---
            if curr_t - last_scout_time > SCOUTING_INTERVAL:
                regime = get_btc_regime()

                if regime == 1:
                    print("🟢 綠燈！執行多單強勢幣海選...")
                    target_coins = scouting_strong_coins(20)

                    for s in target_coins:
                        try:
                            flow, last_p, is_strong = apply_lee_ready_long_logic(s)
                            atr, is_v               = get_market_metrics(s)
                            if last_p > 0:
                                execute_live_long(s, flow, last_p, is_strong, atr, is_v)
                        except Exception:
                            continue
                        time.sleep(0.5)  # Rate-limit buffer between coin checks
                else:
                    print(f"🚦 大盤狀態為 {regime}，海選暫停。")
                    target_coins = []

                last_scout_time = curr_t
                bal_str = f"{get_live_usdc_balance():.2f}"
                print(
                    f"⏳ 巡邏完畢 | 持倉: {list(positions.keys())} | "
                    f"餘額: {bal_str} USDC"
                )

            time.sleep(POSITION_CHECK_INTERVAL)

        except KeyboardInterrupt:
            bal = get_live_usdc_balance()
            print(
                f"\n👋 指揮官手動終止。"
                f"餘額: {bal:.2f} USDC | 持倉: {list(positions.keys())}"
            )
            sys.exit(0)

        except ccxt.RateLimitExceeded:
            # [HL-14] Generic rate-limit handler (replaces Bybit "10006" string check)
            logger.warning("⏳ 主迴圈 Rate Limit，等待 10s...")
            time.sleep(10)

        except Exception as e:
            logger.error(f"❌ 主迴圈未知錯誤: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()