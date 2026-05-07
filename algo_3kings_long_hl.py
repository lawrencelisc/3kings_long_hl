"""
================================================================================
  algo_3kings_long_hl.py
  Migrated from: prototype_long_short_v3.py (Bybit V3 Long/Short → Long-Only)
  Target Exchange: Hyperliquid (via CCXT)
  Strategy: Trend Long-Only Momentum Sniper V3
    - Market Regime     : V3 Multi-asset Trend Detector (ADX+BBW+DI, Long-only simplified)
    - Coin Scout        : TIER1/TIER2 USDC whitelist, ranked by momentum
    - Entry Signal      : Lee-Ready Net Flow + Acceleration + OB Imbalance
    - Tier 2 Gate       : Per-symbol ADX+DI + No-Lag Direction (slope+Donchian) + 1H HTF
    - Risk Management   : ATR-based sizing + Multi-stage Variable Trail SL
    - Regime Confidence : Sensor B progressive sizing (12-bar history)
    - ADX MEI           : Momentum Exhaustion Index (top-protection)
    - Memory            : Dynamic 24h ban system + Cascade SL protection
    - Daily Filter      : Multi-asset 1D structure consensus (no single-asset bias)
    - Direction Detect  : Linear regression slope + Donchian structure (NO EMA, NO LAG)
  Base Currency: USDC (Hyperliquid native)
================================================================================
  KEY MIGRATION CHANGES FROM BYBIT V3:
  [HL-1]  Exchange: ccxt.bybit → ccxt.hyperliquid
          Auth model: apiKey/secret → walletAddress/privateKey
  [HL-2]  Base currency: USDT → USDC throughout
  [HL-3]  Symbol format: BTC/USDT:USDT → BTC/USDC:USDC
  [HL-4]  Balance fetch: ['USDT']['free'] → USDC balance
  [HL-5]  cancel_all_v5() (3 Bybit endpoints + trading_stop) → cancel_all_hl()
            using standard CCXT cancel_all_orders() — HL has no native TP/SL
            attached to position; TP/SL are managed locally by this bot.
  [HL-6]  process_native_exit_log(): replaced Bybit
            private_get_v5_position_closed_pnl with CCXT fetch_my_trades().
  [HL-7]  execute_live_long(): removed private_post_v5_position_trading_stop.
            Bot relies 100% on local trailing-stop logic in manage_long_positions().
  [HL-8]  manage_long_positions(): removed private_post_v5_position_trading_stop
            SL update call; trail SL is now purely software-side.
  [HL-9]  set_leverage(): generalized error handling (HL cross-margin).
  [HL-10] POSITION_CHECK_INTERVAL: 4s → 2s (HL sub-100ms latency).
  [HL-11] Deceleration trigger: accel_z < -2.0 → < -2.5 (HL high-freq noise).
  [HL-12] Blacklist/Whitelist: all :USDT symbols → :USDC equivalents.
  [HL-13] Log label: 'Bybit Native TP/SL' → 'HL Native Exit / Liquidation'.
  [HL-14] Error guard: Bybit "10006" string → ccxt.RateLimitExceeded exception.
  [HL-15] IOC order params: removed 'positionIdx': 0 (Bybit hedge-mode only).
  [HL-16] Leverage error codes: removed "110043"/"110026" guards.
  [HL-17] Regime assets: BTC/USDT:USDT → BTC/USDC:USDC etc.
  [HL-18] Symbol filter: ':USDT' → ':USDC'.
  [HL-19] fetch_positions(): removed params={'category': 'linear'}.
  [HL-20] Position field mapping: HL uses 'entryPx' in info dict, 'szi' for size.
  [HL-21] get_market_metrics(): removed convert_to_bybit_symbol() + category params.
  [HL-22] check_symbol_trend(): removed convert_to_bybit_symbol() + category params.
  [HL-23] Long-only: removed all short code (execute_live_short,
            apply_lee_ready_short_logic, check_flow_health_short,
            sim_open_short, sim_close_short, SHORT_ENTRY_REGIMES).
  [HL-24] FEE_RATE: 0.000389 (HL taker 0.0389%) + FEE_RATE_MAKER=0.000130 (maker 0.013%).
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
from datetime import datetime, timezone


def _influx_noop(*_args, **_kwargs):
    """Influx 未安裝／載入失敗時占位，避免 _safe_influx(fn, ...) 先求值 fn 而 NameError。"""
    pass


# 嘗試導入 Telegram Bot
try:
    from telegram_bot import telegram_notifier
    TELEGRAM_ENABLED = telegram_notifier.enabled
    if TELEGRAM_ENABLED:
        print("✅ Telegram Bot 已成功加載")
    else:
        print("⚠️ Telegram Bot 配置不完整，請檢查 .env 中的 TELEGRAM_BOT_TOKEN 和 TELEGRAM_CHAT_ID")
except ImportError:
    TELEGRAM_ENABLED = False
    print("⚠️ Telegram Bot 模塊未找到，通知功能將禁用")
except Exception as e:
    TELEGRAM_ENABLED = False
    print(f"⚠️ Telegram Bot 初始化失敗: {e}")

# 嘗試導入 InfluxDB Writer
try:
    import sys as _sys
    import os as _os
    _sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), 'utils'))
    from influx_writer import (
        write_regime   as _influx_write_regime,
        write_trade    as _influx_write_trade,
        write_balance  as _influx_write_balance,
        write_position as _influx_write_position,
        INFLUX_ENABLED,
    )
    if INFLUX_ENABLED:
        print("✅ InfluxDB Writer 已成功加載")
    else:
        print("⚠️ InfluxDB 已停用（INFLUX_ENABLED=false 或連線失敗）")
except ImportError:
    INFLUX_ENABLED = False
    print("⚠️ InfluxDB Writer 模塊未找到，監控功能將禁用")
    _influx_write_regime = _influx_noop
    _influx_write_trade = _influx_noop
    _influx_write_balance = _influx_noop
    _influx_write_position = _influx_noop
except Exception as e:
    INFLUX_ENABLED = False
    print(f"⚠️ InfluxDB Writer 初始化失敗: {e}")
    _influx_write_regime = _influx_noop
    _influx_write_trade = _influx_noop
    _influx_write_balance = _influx_noop
    _influx_write_position = _influx_noop


def _safe_influx(fn, *args, **kwargs):
    """呼叫 InfluxDB 寫入函數，失敗時靜默，不影響主策略。"""
    if not INFLUX_ENABLED:
        return
    try:
        fn(*args, **kwargs)
    except Exception:
        pass


# ==========================================
# ⚙️ Logger & Exchange Initialization
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('AlgoTrade_Long_HL_V3')

from dotenv import load_dotenv
load_dotenv()

# Telegram 發送總開關（需在 .env 設 ENABLE_TELEGRAM_SEND=true 才會真的發送）
ENABLE_TELEGRAM_SEND = os.getenv('ENABLE_TELEGRAM_SEND', 'false').lower() == 'true'

# [HL-1] Hyperliquid wallet-based auth:
#         HL_WALLET_ADDRESS = public wallet address (e.g. 0xABCD...)
#         HL_PRIVATE_KEY    = 64-char hex private key
API_KEY    = os.getenv('HL_WALLET_ADDRESS')
API_SECRET = os.getenv('HL_PRIVATE_KEY')

if not API_KEY or not API_SECRET:
    logger.error("❌ API keys not found! Please set HL_WALLET_ADDRESS and HL_PRIVATE_KEY in .env file.")
    sys.exit(1)

# [HL-1] Instantiate Hyperliquid via CCXT
exchange = ccxt.hyperliquid({
    'walletAddress': API_KEY,
    'privateKey':    API_SECRET,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap',
        'user': API_KEY,
    }
})

# load_markets() with retry
max_retries = 3
for _retry in range(max_retries):
    try:
        exchange.load_markets()
        logger.info("✅ 交易所市場信息加載成功 (Hyperliquid)")
        break
    except Exception as e:
        if _retry == max_retries - 1:
            logger.error(f"❌ 加載市場信息失敗 {max_retries} 次: {str(e)[:100]}...")
            logger.warning("⚠️ 將嘗試繼續運行，但某些功能可能受限")
        else:
            _wait = 2 ** _retry
            logger.warning(f"⚠️ 加載市場信息失敗 (嘗試 {_retry+1}/{max_retries}): {str(e)[:100]}...")
            logger.warning(f"   等待 {_wait} 秒後重試...")
            time.sleep(_wait)


# ==========================================
# ⚙️ [FIX-SIM] Simulation Mode
# ==========================================
SIMULATION_MODE = os.getenv('SIMULATION_MODE', 'false').lower() == 'true'

SIM_INITIAL_BALANCE = float(os.getenv('SIM_BALANCE', '1000.0'))
sim_balance:     float = SIM_INITIAL_BALANCE
sim_equity:      float = SIM_INITIAL_BALANCE
sim_positions:   dict  = {}
sim_trade_count: int   = 0
sim_total_pnl:   float = 0.0


# ==========================================
# 📁 File Paths
# ==========================================
LOG_DIR    = "result"
STATUS_DIR = "status"

_mode_tag      = "sim" if SIMULATION_MODE else "live"
LOG_FILE       = f"{LOG_DIR}/{_mode_tag}_long_hl_log.csv"
STATUS_FILE    = f"{STATUS_DIR}/btc_regime_long_hl.csv"
BLACKLIST_FILE = f"{STATUS_DIR}/dynamic_blacklist_long_hl.json"

if not os.path.exists(LOG_DIR):    os.makedirs(LOG_DIR)
if not os.path.exists(STATUS_DIR): os.makedirs(STATUS_DIR)

if SIMULATION_MODE:
    print("=" * 60)
    print("🔵 SIMULATION MODE 已啟動 (Hyperliquid Long V3)")
    print(f"   初始資金  : ${SIM_INITIAL_BALANCE:.2f} USDC")
    print(f"   交易日誌  : {LOG_FILE}")
    print(f"   數據來源  : Hyperliquid 真實行情（公開 API）")
    print("=" * 60)


# ==========================================
# ⚙️ In-Memory State
# ==========================================
positions:          dict  = {}
cooldown_tracker:   dict  = {}
consecutive_losses: dict  = {}

# [V4-MOMENTUM] Removed:
#   _adx_history          (fed MEI top-protection gate — gate removed)
#   _regime_signal_history (fed Sensor B ramp — late-entry magnifier removed)
#   recent_sl_times       (fed Cascade SL freeze — was a band-aid for top-chasing)

# Telegram: track last notification
_last_market_signal            = 0
_last_market_notification_time = 0


# ==========================================
# ⚙️ Strategy Parameters
# ==========================================
WORKING_CAPITAL        = 400.0
MAX_LEVERAGE           = 5.0
RISK_PER_TRADE         = 0.005
MIN_NOTIONAL           = 10.0         # [HL-2] HL minimum ~$10 USDC
MAX_NOTIONAL_PER_TRADE = 40.0

# [V4-MOMENTUM] R:R reversed: TP=3×ATR / SL=1.2×ATR → 2.5:1 (was 0.875:1)
# Old (3.5/4.0) requires 53% WR breakeven; new (3.0/1.2) only needs 28.6% WR.
NET_FLOW_SIGMA = 1.2
TP_ATR_MULT    = 3.0
SL_ATR_MULT    = 1.2

# [V4-MOMENTUM] Anti-chase: skip new entries if BTC already pumped > MAX_30MIN_PUMP_PCT
# in the last 30 minutes (catches trend FORMATION, not late confirmation).
MAX_30MIN_PUMP_PCT = float(os.getenv('MAX_30MIN_PUMP_PCT', '0.015'))   # 1.5%

# [V4-MOMENTUM] ADX rising threshold (per 5m bar) — formation signal, NOT confirmation
ADX_RISING_THR = float(os.getenv('ADX_RISING_THR', '0.5'))

MAX_CONSECUTIVE_LOSSES = 3
DYNAMIC_BAN_DURATION   = 86400

# [HL-24] HL fee: taker 0.0389% / maker 0.013%  (all IOC entries/exits → taker rate)
FEE_RATE       = float(os.getenv('FEE_RATE',       '0.000389'))  # taker (IOC orders)
FEE_RATE_MAKER = float(os.getenv('FEE_RATE_MAKER', '0.000130'))  # maker (limit, ref only)

MAX_CONCURRENT_POSITIONS = 5

SCOUTING_INTERVAL = 125
# [HL-10] Reduced from 4s: HL sub-100ms latency allows tighter monitoring
POSITION_CHECK_INTERVAL = 2

# [V4-MOMENTUM] Tightened from 90min → 30min. Momentum trades that don't work in
# 30min are unlikely to recover; holding longer just bleeds opportunity cost.
TIMEOUT_SECONDS      = 1800     # 30 min hard timeout
TIMEOUT_LOSS_FLOOR   = -0.003   # immediate exit if loss > 0.3%

# [HL-23] Long-only: only regime_signal +2 (Trend Long) is acted upon
ACTIVE_LONG_SIGNALS = [2]

# Coin scouting filters
MIN_VOLUME_USDC      = float(os.getenv('MIN_VOLUME_USDC',      '5_000_000'))  # 500萬 USDC/day floor
MAX_SCOUT_CHANGE_PCT = float(os.getenv('MAX_SCOUT_CHANGE_PCT', '8.0'))        # tightened: 20→8% daily


# ==========================================
# ⚙️ Process Guardian / Heartbeat
# ==========================================
# SL is 100% software-side on HL (no exchange-native SL).
# The bot MUST run under a process guardian (start.sh / systemd / pm2).
# Heartbeat file lets the guardian detect hangs (not just crashes).
HEARTBEAT_FILE     = os.getenv('HEARTBEAT_FILE', '/tmp/algo_3kings_hl.heartbeat')
HEARTBEAT_INTERVAL = 30   # seconds between heartbeat writes


def write_heartbeat() -> None:
    """Write JSON heartbeat so an external watchdog can detect hangs."""
    try:
        payload = {
            'ts':        time.time(),
            'iso':       datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'positions': len(positions),
            'sim_mode':  SIMULATION_MODE,
        }
        with open(HEARTBEAT_FILE, 'w') as f:
            json.dump(payload, f)
    except Exception:
        pass   # never let a heartbeat write crash the main loop


_last_heartbeat_ts: float = 0.0


# ==========================================
# ⚙️ Cache Settings
# ==========================================
REGIME_CACHE_TTL       = 60
POSITIONS_CACHE_TTL    = 8
ATR_CACHE_TTL          = 60
SYMBOL_TREND_CACHE_TTL = 60

_regime_cache:       dict = {'data': None, 'ts': 0}
_positions_cache:    dict = {'data': None, 'ts': 0}
_atr_cache:          dict = {}
_symbol_trend_cache: dict = {}


# ==========================================
# ⚙️ [HL-12] Blacklist & Whitelist (USDC symbols)
# ==========================================
BLACKLIST = [
    'USDC/USDC:USDC', 'DAI/USDC:USDC',   'FDUSD/USDC:USDC', 'BUSD/USDC:USDC',
    'TUSD/USDC:USDC', 'PYUSD/USDC:USDC', 'USDP/USDC:USDC',  'EURS/USDC:USDC',
    'USDE/USDC:USDC', 'USAT/USDC:USDC',  'USD0/USDC:USDC',  'USTC/USDC:USDC',
    'LUSD/USDC:USDC', 'FRAX/USDC:USDC',  'MIM/USDC:USDC',   'RLUSD/USDC:USDC',
    'WBTC/USDC:USDC', 'WETH/USDC:USDC',  'WBNB/USDC:USDC',  'WAVAX/USDC:USDC',
    'stETH/USDC:USDC', 'cbETH/USDC:USDC', 'WHT/USDC:USDC',
]

# [HL-12] TIER1: High-liquidity majors (full position sizing)
TIER1_WHITELIST = [
    'BTC/USDC:USDC', 'ETH/USDC:USDC', 'SOL/USDC:USDC', 'BNB/USDC:USDC',
    'XRP/USDC:USDC', 'ADA/USDC:USDC', 'AVAX/USDC:USDC', 'DOGE/USDC:USDC',
    'DOT/USDC:USDC', 'MATIC/USDC:USDC', 'LINK/USDC:USDC', 'UNI/USDC:USDC',
    'PEPE/USDC:USDC', 'SHIB/USDC:USDC', 'ARB/USDC:USDC', 'OP/USDC:USDC',
    'APT/USDC:USDC', 'SUI/USDC:USDC', 'NEAR/USDC:USDC', 'ATOM/USDC:USDC',
]

# [HL-12] TIER2: Mid-cap coins (auto-scaled to 60% position size)
TIER2_WHITELIST = [
    'TIA/USDC:USDC', 'INJ/USDC:USDC', 'LDO/USDC:USDC', 'AAVE/USDC:USDC',
    'BCH/USDC:USDC', 'LTC/USDC:USDC', 'TON/USDC:USDC', 'TRX/USDC:USDC',
    'HBAR/USDC:USDC', 'FIL/USDC:USDC', 'ICP/USDC:USDC', 'IMX/USDC:USDC',
    'SEI/USDC:USDC', 'WIF/USDC:USDC', 'BONK/USDC:USDC', 'JUP/USDC:USDC',
    'TAO/USDC:USDC', 'ETC/USDC:USDC', 'ENA/USDC:USDC', 'ONDO/USDC:USDC',
]

WHITELIST             = TIER1_WHITELIST + TIER2_WHITELIST
TIER2_SET             = frozenset(TIER2_WHITELIST)
TIER2_SIZE_MULTIPLIER = 0.6


# ==========================================
# ⚙️ CSV Column Schemas
# ==========================================
CSV_COLUMNS = [
    'timestamp', 'symbol', 'action', 'price', 'amount', 'trade_value',
    'atr', 'net_flow', 'tp_price', 'sl_price', 'reason',
    'realized_pnl', 'actual_balance', 'effective_balance',
    'sim_mode', 'sim_equity', 'sim_total_pnl',
    'regime_signal', 'mean_adx', 'market_score',
]
STATUS_COLUMNS = [
    'timestamp', 'btc_price', 'target_price', 'hma20', 'hma50',
    'adx', 'signal_code', 'decision_text',
    'mean_ndi', 'mean_pdi', 'ndi_slope', 'pdi_slope',
    'ndi_rising', 'pdi_rising', 'ndipdi',
    'score', 'ema_dir', 'is_bear',
]


# ==========================================
# 🛠️ [MODULE 1] CSV Logging
# ==========================================
def log_to_csv(data_dict: dict) -> None:
    row = {col: '' for col in CSV_COLUMNS}
    row.update(data_dict)
    row['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if SIMULATION_MODE:
        row['sim_mode']      = 'SIM'
        row['sim_equity']    = round(sim_equity, 4)
        row['sim_total_pnl'] = round(sim_total_pnl, 4)
    pd.DataFrame([row], columns=CSV_COLUMNS).to_csv(
        LOG_FILE, mode='a', index=False, header=not os.path.exists(LOG_FILE)
    )


def log_status_to_csv(data_dict: dict) -> None:
    row = {col: '' for col in STATUS_COLUMNS}
    row.update(data_dict)
    row['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pd.DataFrame([row], columns=STATUS_COLUMNS).to_csv(
        STATUS_FILE, mode='a', index=False, header=not os.path.exists(STATUS_FILE)
    )


# ==========================================
# 🔵 [MODULE 2] Simulation Ledger (Long-Only)
# ==========================================
def sim_open_long(symbol: str, amount: float, price: float) -> tuple:
    """Simulate long entry: deduct cost + taker fee from sim_balance."""
    global sim_balance, sim_trade_count
    fee  = amount * price * FEE_RATE
    cost = amount * price + fee
    if sim_balance < cost:
        logger.warning(f"🔵 [SIM] {symbol} 餘額不足 (需 {cost:.2f}, 有 {sim_balance:.2f})")
        return 0, 0
    sim_balance -= cost
    sim_trade_count += 1
    logger.info(f"🔵 [SIM] OPEN LONG {symbol} | 數量:{amount} @ {price:.4f} | 費用:{fee:.4f} | 餘額:{sim_balance:.2f}")
    return amount, price


def _sim_calc_equity(exclude_symbol: str = None) -> float:
    """Compute Sim account total equity including unrealized long PnL."""
    unrealized = 0.0
    for s, p in sim_positions.items():
        if s == exclude_symbol:
            continue
        try:
            curr       = exchange.fetch_ticker(s)['last']
            unrealized += (curr - p['entry_price']) * p['amount']
        except Exception:
            pass
    return sim_balance + unrealized


def sim_close_long(symbol: str, amount: float, price: float) -> float:
    """Simulate long exit: credit proceeds and compute realized PnL."""
    global sim_balance, sim_total_pnl, sim_equity, sim_trade_count
    if symbol not in sim_positions:
        logger.warning(f"🔵 [SIM] {symbol} 找不到持倉，無法平倉")
        return 0.0
    pos       = sim_positions[symbol]
    fee       = amount * price * FEE_RATE
    gross_pnl = (price - pos['entry_price']) * amount
    net_pnl   = gross_pnl - fee
    sim_balance   += amount * price - fee
    sim_total_pnl += net_pnl
    sim_trade_count += 1
    sim_equity = _sim_calc_equity(exclude_symbol=symbol)
    logger.info(
        f"🔵 [SIM] CLOSE LONG {symbol} | 出場:{price:.4f} 入場:{pos['entry_price']:.4f} "
        f"| PnL:{net_pnl:+.4f} | 總PnL:{sim_total_pnl:+.4f} | 餘額:{sim_balance:.2f}"
    )
    return round(net_pnl, 4)


def sim_get_positions() -> list:
    """Convert sim_positions to exchange.fetch_positions() compatible format."""
    result = []
    for symbol, pos in sim_positions.items():
        result.append({
            'symbol':      symbol,
            'side':        'long',
            'contracts':   pos['amount'],
            'entryPrice':  pos['entry_price'],
            'stopLoss':    pos.get('sl_price', 0),
            'takeProfit':  pos.get('tp_price', 0),
            'info': {
                'side':    'Buy',
                'szi':     str(pos['amount']),
                'entryPx': str(pos['entry_price']),
            },
            'createdTime': pos.get('entry_time', time.time()) * 1000,
        })
    return result


def sim_report() -> None:
    """Print Simulation performance summary."""
    global sim_equity
    unrealized = 0.0
    for symbol, pos in sim_positions.items():
        try:
            curr_p     = exchange.fetch_ticker(symbol)['last']
            unrealized += (curr_p - pos['entry_price']) * pos['amount']
        except Exception:
            pass
    sim_equity = sim_balance + unrealized
    roi = (sim_equity - SIM_INITIAL_BALANCE) / SIM_INITIAL_BALANCE * 100
    print("=" * 60)
    print("📊 [SIM] 績效摘要 (Hyperliquid Long V3)")
    print(f"   初始資金       : ${SIM_INITIAL_BALANCE:.2f}")
    print(f"   可用餘額       : ${sim_balance:.2f}")
    print(f"   未實現 PnL     : ${unrealized:+.4f}")
    print(f"   總資產 (Equity): ${sim_equity:.2f}")
    print(f"   累計已實現 PnL : ${sim_total_pnl:+.4f}")
    print(f"   ROI            : {roi:+.2f}%")
    print(f"   總成交筆數     : {sim_trade_count}")
    print(f"   當前持倉       : {list(sim_positions.keys())}")
    print("=" * 60)


# ==========================================
# 🛠️ [MODULE 3] Account & Order Utilities
# ==========================================
def get_live_usdc_balance() -> float:
    """
    [FIX-SIM] Sim mode returns local balance; Live mode fetches from HL.
    [HL-4] Base currency: USDT → USDC.
    """
    if SIMULATION_MODE:
        return sim_balance
    try:
        bal = exchange.fetch_balance({'type': 'swap', 'user': API_KEY})
        usdc_free = (bal.get('USDC', {}).get('free', 0) or
                     bal.get('total', {}).get('USDC', 0) or 0)
        return float(usdc_free)
    except Exception as e:
        logger.error(f"❌ Failed to fetch balance: {e}")
        return 0.0


def cancel_all_hl(symbol: str) -> None:
    """
    Cancel all open orders for a symbol on Hyperliquid.
    [HL-5] Replaces Bybit cancel_all_v5() (3 cancel variants + trading_stop).
           HL uses standard CCXT cancel_all_orders().
    [FIX-SIM] Sim mode: no orders to cancel, skip silently.
    """
    if SIMULATION_MODE:
        return
    try:
        exchange.cancel_all_orders(symbol)
        logger.debug(f"🧹 {symbol} 所有掛單已撤銷 (HL)")
    except Exception as e:
        logger.debug(f"⚠️ {symbol} 撤單失敗 (non-critical): {e}")


def get_3_layer_avg_price(symbol: str, side: str = 'bids') -> float:
    """Volume-weighted average price of top 3 order book levels."""
    try:
        ob     = exchange.fetch_order_book(symbol, limit=5)
        levels = ob[side][:3]
        if not levels:
            return None
        return sum(level[0] for level in levels) / len(levels)
    except Exception:
        return None


def get_market_metrics(symbol: str) -> tuple:
    """
    ATR(14) on 5m chart with 60s cache.
    [HL-21] Removed convert_to_bybit_symbol() and params={'category': 'linear'}.
            HL uses CCXT symbol directly with no extra params.
    """
    cached = _atr_cache.get(symbol)
    if cached and (time.time() - cached['ts']) < ATR_CACHE_TTL:
        return cached['atr'], cached['is_volatile']

    for retry in range(2):
        try:
            # [HL-21] Direct CCXT symbol — no Bybit-specific conversion or params
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe='5m', limit=100)
            df    = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
            df['tr'] = np.maximum(
                df['h'] - df['l'],
                np.maximum(abs(df['h'] - df['c'].shift(1)), abs(df['l'] - df['c'].shift(1)))
            )
            atr         = df['tr'].rolling(14, min_periods=1).mean().iloc[-1]
            is_volatile = (atr / df['c'].iloc[-1]) > 0.0015
            if pd.isna(atr) or atr == 0:
                return None, False
            _atr_cache[symbol] = {'atr': atr, 'is_volatile': is_volatile, 'ts': time.time()}
            return atr, is_volatile
        except Exception as e:
            if retry == 1:
                logger.warning(f"⚠️ {symbol} ATR計算失敗: {str(e)[:80]}")
                return None, False
            time.sleep(2)
    return None, False


def fetch_tickers_for_positions(symbols: list) -> dict:
    """Batch-fetch current prices for all held positions."""
    if not symbols:
        return {}
    try:
        result = exchange.fetch_tickers(symbols)
        return {s: t['last'] for s, t in result.items() if t.get('last')}
    except Exception as e:
        logger.warning(f"⚠️ batch fetch_tickers 失敗，逐一降級: {e}")
        prices = {}
        for s in symbols:
            try:
                prices[s] = exchange.fetch_ticker(s)['last']
                time.sleep(0.05)
            except Exception:
                pass
        return prices


# ==========================================
# 🛠️ [MODULE 4] Dynamic Ban System (JSON-Persistent)
# ==========================================
def save_dynamic_blacklist() -> None:
    data = {'consecutive_losses': consecutive_losses, 'cooldown_tracker': cooldown_tracker}
    try:
        with open(BLACKLIST_FILE, 'w') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        logger.error(f"❌ 儲存動態黑名單失敗: {e}")


def load_dynamic_blacklist() -> None:
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
            print(f"✅ 成功讀取 JSON 記憶！目前有 {banned_count} 隻妖幣處於 24 小時封禁中。")
        except Exception as e:
            logger.error(f"❌ 讀取動態黑名單失敗: {e}")
    else:
        print("ℹ️ 找不到歷史 JSON 記憶，以全新白紙狀態啟動。")


def handle_trade_result(symbol: str, pnl: float, is_sl_exit: bool = False) -> None:
    """
    Update consecutive-loss counter and apply dynamic 24h ban if threshold reached.
    [V4-MOMENTUM] is_sl_exit kept as kwarg for callsite compatibility but no longer
    feeds Cascade SL tracking (Cascade SL freeze removed — see strategy params).
    """
    global consecutive_losses, cooldown_tracker
    _ = is_sl_exit  # accepted but unused; kept for API compatibility
    if pnl > 0:
        consecutive_losses[symbol] = 0
        if symbol in cooldown_tracker:
            del cooldown_tracker[symbol]
        print(f"🏆 {symbol} 贏錢平倉！解除冷卻，允許乘勝追擊！")
    elif pnl < 0:
        consecutive_losses[symbol] = consecutive_losses.get(symbol, 0) + 1
        if consecutive_losses[symbol] >= MAX_CONSECUTIVE_LOSSES:
            cooldown_tracker[symbol] = time.time() + DYNAMIC_BAN_DURATION
            print(f"🚫 [動態封禁] {symbol} 已連續虧損 {consecutive_losses[symbol]} 次！打入冷宮 24 小時。")
        else:
            cooldown_tracker[symbol] = max(
                cooldown_tracker.get(symbol, 0), time.time() + 480
            )
    save_dynamic_blacklist()


# ==========================================
# 🛠️ [MODULE 5] Unified Positions Interface (Sim/Live)
# ==========================================
def get_live_positions_cached() -> list:
    """
    Unified positions fetch with cache.
    [FIX-SIM] Sim mode: return local sim_positions (no cache TTL needed).
    [HL-19] Live mode: no params={'category': 'linear'} for HL.
    """
    if SIMULATION_MODE:
        return sim_get_positions()

    if ((time.time() - _positions_cache['ts']) < POSITIONS_CACHE_TTL
            and _positions_cache['data'] is not None):
        return _positions_cache['data']
    try:
        # [HL-19] No category param needed for Hyperliquid
        data = exchange.fetch_positions()
        _positions_cache['data'] = data
        _positions_cache['ts']   = time.time()
        return data
    except Exception as e:
        logger.warning(f"⚠️ fetch_positions 失敗: {e}")
        return _positions_cache['data'] or []


# [V4-MOMENTUM] No-Lag Direction Detector REMOVED.
# Old _slope_norm + _donchian_structure + _no_lag_direction + _multi_asset_direction_consensus
# fed the 5m / 1H / 1D direction filters that were proven to be top-chasing
# (correlation with fwd 30m return: -0.12 / 0 / 0). New trend-formation logic uses
# ADX velocity + breakout detection at the symbol level instead.


# ==========================================
# 🎯 [MODULE 6] Per-Symbol Trend Gate (Tier 2)
# ==========================================
def check_symbol_trend(symbol: str) -> dict:
    """
    [V4-MOMENTUM] Per-symbol gate — REWRITTEN to detect trend FORMATION + breakout,
                  not late confirmation.

    OLD logic: ADX>=22 + DI_spread>=3 + 5m direction=+1 + 1H HTF=+1
               → all 4 are lagging confirmations; entries hit local tops.
    NEW logic: ADX rising AND price near/above 20-bar high
               → catches breakouts as they form, before extension.

    Cache: 60 seconds.
    """
    cached = _symbol_trend_cache.get(symbol)
    if cached and (time.time() - cached['ts']) < SYMBOL_TREND_CACHE_TTL:
        return cached['data']

    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe='5m', limit=80)
        if len(ohlcv) < 30:
            result = {'is_long_ok': False, 'reason': 'insufficient data'}
            _symbol_trend_cache[symbol] = {'data': result, 'ts': time.time()}
            return result

        df     = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
        highs  = df['h'].values.astype(float)
        lows   = df['l'].values.astype(float)
        closes = df['c'].values.astype(float)

        prev_h = np.roll(highs, 1);  prev_h[0] = highs[0]
        prev_l = np.roll(lows, 1);   prev_l[0] = lows[0]
        prev_c = np.roll(closes, 1); prev_c[0] = closes[0]
        tr     = np.maximum(highs - lows, np.maximum(np.abs(highs - prev_c), np.abs(lows - prev_c)))
        up     = highs - prev_h
        dn     = prev_l - lows
        pdm    = np.where((up > dn) & (up > 0), up, 0.0)
        ndm    = np.where((dn > up) & (dn > 0), dn, 0.0)

        win   = 14
        atr_s = pd.Series(tr).ewm(alpha=1.0 / win, adjust=False).mean().values
        pdm_s = pd.Series(pdm).ewm(alpha=1.0 / win, adjust=False).mean().values
        ndm_s = pd.Series(ndm).ewm(alpha=1.0 / win, adjust=False).mean().values
        with np.errstate(divide='ignore', invalid='ignore'):
            pdi = np.where(atr_s > 0, 100.0 * pdm_s / atr_s, 0.0)
            ndi = np.where(atr_s > 0, 100.0 * ndm_s / atr_s, 0.0)
            dx  = np.where((pdi + ndi) > 0, 100.0 * np.abs(pdi - ndi) / (pdi + ndi), 0.0)
        adx_arr = pd.Series(dx).ewm(alpha=1.0 / win, adjust=False).mean().values

        # [V4] Formation signals
        adx_now      = float(adx_arr[-1])
        adx_5bar_ago = float(adx_arr[-6]) if len(adx_arr) >= 6 else adx_now
        adx_rising   = (adx_now - adx_5bar_ago) > ADX_RISING_THR

        # Breakout detector: current close near or above 20-bar high
        n_break = min(20, len(closes) - 1)
        recent_high = float(np.max(highs[-n_break:-1])) if n_break > 1 else closes[-1]
        breakout    = closes[-1] >= recent_high * 0.999    # within 0.1% of new high

        # Anti-chase: this symbol's 30min pump (6 bars on 5m chart)
        sym_30m_pump = float(closes[-1] / closes[-7] - 1) if len(closes) > 7 else 0.0
        not_extended = sym_30m_pump < (MAX_30MIN_PUMP_PCT * 1.5)   # symbol-level looser than BTC

        di_spread   = float(pdi[-1]) - float(ndi[-1])
        di_ok       = di_spread >= 0.0   # PDI weakly dominant — relaxed from old +3.0/+5.0

        is_long_ok = adx_rising and breakout and not_extended and di_ok

        result = {
            'is_long_ok':   is_long_ok,
            'adx':          round(adx_now, 2),
            'adx_velocity': round(adx_now - adx_5bar_ago, 2),
            'di_spread':    round(di_spread, 2),
            'breakout':     breakout,
            'sym_30m_pump': round(sym_30m_pump * 100, 2),
            'reason':       (
                f"ΔADX={adx_now-adx_5bar_ago:+.1f} brk={int(breakout)} "
                f"30m={sym_30m_pump*100:+.2f}% DI={di_spread:+.1f}"
            ),
        }
        _symbol_trend_cache[symbol] = {'data': result, 'ts': time.time()}
        return result

    except Exception as e:
        logger.debug(f"check_symbol_trend({symbol}) 失敗: {str(e)[:80]}")
        return {'is_long_ok': False, 'reason': f'error: {str(e)[:60]}'}


# ==========================================
# 🧠 [MODULE 7] Market Regime Detector V3 (HL)
# ==========================================
def get_btc_regime_v3_fast() -> dict:
    """
    Multi-asset bidirectional regime detector V6.7 (BugFixed).
    [HL-17] All regime assets use /USDC:USDC symbols.
    [HL-19] No params={'category': 'linear'} for HL.
    [HL-21] No convert_to_bybit_symbol().
    Signals: +2=Trend Long, +1=MR Long, 0=Neutral, -1/-2/-3=Short variants.
    Long-only: only +2 is acted upon (ACTIVE_LONG_SIGNALS = [2]).
    """
    if ((time.time() - _regime_cache['ts']) < REGIME_CACHE_TTL
            and _regime_cache['data'] is not None):
        return _regime_cache['data']

    try:
        TIMEFRAME   = '5m'
        OHLCV_LIMIT = 300

        ADX_WIN    = 14
        BB_WIN     = 20
        ATR_WIN    = 14
        # [WIN-NLG] EMA_WIN / EMA_SLOPE_BARS removed — replaced by no-lag direction
        # detector (linear regression slope + Donchian structure dual-confirm)

        TR_BB_PCT      = 30
        HVOL_ATR_PCT   = 90

        RET_7D_BARS        = 288
        MACRO_BEAR_RTN_THR = -0.03
        MACRO_BULL_RTN_THR = +0.02

        # [HL-17] Regime assets updated to USDC-denominated symbols
        REGIME_ASSETS = [
            'BTC/USDC:USDC', 'ETH/USDC:USDC', 'SOL/USDC:USDC',
            'BNB/USDC:USDC', 'XRP/USDC:USDC', 'AVAX/USDC:USDC',
            'ADA/USDC:USDC', 'DOGE/USDC:USDC',
        ]

        def rolling_adx_simple(highs, lows, closes, win=ADX_WIN):
            n   = len(closes)
            adx = np.full(n, 25.0)
            pdi = np.full(n, 25.0)
            ndi = np.full(n, 25.0)
            prev_h = np.roll(highs, 1);  prev_h[0] = highs[0]
            prev_l = np.roll(lows, 1);   prev_l[0] = lows[0]
            prev_c = np.roll(closes, 1); prev_c[0] = closes[0]
            hl  = highs - lows
            hpc = np.abs(highs - prev_c)
            lpc = np.abs(lows - prev_c)
            tr  = np.maximum(hl, np.maximum(hpc, lpc)); tr[0] = hl[0]
            up  = highs - prev_h
            dn  = prev_l - lows
            pdm = np.where((up > dn) & (up > 0), up, 0.0)
            ndm = np.where((dn > up) & (dn > 0), dn, 0.0)
            if n > win:
                atr_s = np.zeros(n); pdm_s = np.zeros(n); ndm_s = np.zeros(n)
                atr_s[win] = tr[1:win+1].sum()
                pdm_s[win] = pdm[1:win+1].sum()
                ndm_s[win] = ndm[1:win+1].sum()
                for i in range(win+1, n):
                    atr_s[i] = atr_s[i-1] - atr_s[i-1]/win + tr[i]
                    pdm_s[i] = pdm_s[i-1] - pdm_s[i-1]/win + pdm[i]
                    ndm_s[i] = ndm_s[i-1] - ndm_s[i-1]/win + ndm[i]
                with np.errstate(divide='ignore', invalid='ignore'):
                    _pdi = np.where(atr_s > 0, 100*pdm_s/atr_s, 0.0)
                    _ndi = np.where(atr_s > 0, 100*ndm_s/atr_s, 0.0)
                    dx   = np.where((_pdi+_ndi) > 0, 100*np.abs(_pdi-_ndi)/(_pdi+_ndi), 0.0)
                adx[2*win] = dx[win:2*win].mean()
                for i in range(2*win+1, n):
                    adx[i] = (adx[i-1]*(win-1) + dx[i]) / win
                adx[:2*win] = adx[2*win]
                pdi[win:] = _pdi[win:]; pdi[:win] = _pdi[win]
                ndi[win:] = _ndi[win:]; ndi[:win] = _ndi[win]
            return adx, pdi, ndi

        def rolling_bbwidth_fast(closes, win=BB_WIN):
            s   = pd.Series(closes)
            mid = s.rolling(win).mean()
            std = s.rolling(win).std(ddof=0)
            bbw = (4 * std / mid.replace(0, np.nan)).fillna(0.0).values.copy()
            fv  = win - 1
            if len(bbw) > fv and bbw[fv] != 0.0:
                bbw[:fv] = bbw[fv]
            return bbw

        def rolling_atr_pct_fast(highs, lows, closes, win=ATR_WIN):
            prev_c = np.roll(closes, 1); prev_c[0] = closes[0]
            tr     = np.maximum(highs-lows, np.maximum(np.abs(highs-prev_c), np.abs(lows-prev_c)))
            tr[0]  = highs[0] - lows[0]
            atr    = pd.Series(tr).ewm(span=win, adjust=False).mean().values
            return np.where(closes > 0, atr/closes, 0.0)

        def rolling_return(closes, win=RET_7D_BARS):
            n   = len(closes)
            ret = np.zeros(n)
            if n <= win:
                return ret
            prev  = closes[:-win]
            curr  = closes[win:]
            valid = prev > 0
            ret[win:] = np.where(valid, (curr - prev) / prev, 0.0)
            return ret

        print("📊 開始計算市場狀態信號 (Hyperliquid)...")
        regime_data = {}
        all_bbw     = []
        all_atr_pct = []
        # [V4-MOMENTUM] 1D daily filter REMOVED — confirmed top-chasing on 5m timeframe
        # (correlation of multi-day directional confirmation with fwd 30m return: ≈ 0)

        for sym in REGIME_ASSETS:
            try:
                # [HL-21] Direct CCXT symbol — no convert_to_bybit_symbol, no category params
                ohlcv = exchange.fetch_ohlcv(sym, timeframe=TIMEFRAME, limit=OHLCV_LIMIT)
                if len(ohlcv) < 100:
                    continue

                df     = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
                closes = df['c'].values.astype(float)
                highs  = df['h'].values.astype(float)
                lows   = df['l'].values.astype(float)

                adx, pdi, ndi = rolling_adx_simple(highs, lows, closes)
                bbw       = rolling_bbwidth_fast(closes)
                atr_pct   = rolling_atr_pct_fast(highs, lows, closes)
                ret_arr   = rolling_return(closes, RET_7D_BARS)

                regime_data[sym] = {
                    'closes': closes, 'highs': highs, 'lows': lows,
                    'adx': adx, 'bbw': bbw,
                    'atr_pct': atr_pct, 'ret': ret_arr, 'pdi': pdi, 'ndi': ndi,
                }

                start_idx = max(0, len(closes) - 50)
                all_bbw.extend(bbw[start_idx:].tolist())
                all_atr_pct.extend(atr_pct[start_idx:].tolist())

            except Exception as e:
                logger.warning(f"⚠️ {sym} 指標計算失敗: {e}")
                continue

        if not regime_data:
            result = {'signal': 0, 'brake': False, 'soft_brake': False,
                      'brake_reason': 'No data', 'regime_signal': 0}
            _regime_cache['data'] = result
            _regime_cache['ts']   = time.time()
            return result

        def safe_pct(arr, p):
            a = np.asarray(arr)
            return float(np.percentile(a, p)) if len(a) > 0 else 0.0

        bb_thr = safe_pct(all_bbw,     TR_BB_PCT)
        atr_hi = safe_pct(all_atr_pct, HVOL_ATR_PCT)

        # [V4-MOMENTUM] Collect last + 5-bar-ago snapshots for 'ADX rising' detection.
        # Old code collected only DI for "PDI rising" check — that filter had ≈0 correlation
        # with forward returns; we replace it with multi-bar ADX velocity instead.
        last_adx     = []; last_bbw     = []; last_atr      = []
        last_ndipdi  = []; last_ndi     = []; last_pdi      = []
        adx_5bar_ago = []; bbw_5bar_ago = []
        btc_30m_pump = 0.0   # populated below from BTC closes

        for sym, data in regime_data.items():
            idx = len(data['closes']) - 1
            if idx >= 0:
                last_adx.append(data['adx'][idx])
                last_bbw.append(data['bbw'][idx])
                last_atr.append(data['atr_pct'][idx])
                last_ndipdi.append(data['ndi'][idx] - data['pdi'][idx])
                last_ndi.append(data['ndi'][idx])
                last_pdi.append(data['pdi'][idx])
                # 5-bar = 25min on 5m chart — captures trend formation, not just last bar
                prev_idx5 = max(0, idx - 5)
                adx_5bar_ago.append(data['adx'][prev_idx5])
                bbw_5bar_ago.append(data['bbw'][prev_idx5])

        if not last_adx:
            result = {'signal': 0, 'brake': False, 'soft_brake': False,
                      'brake_reason': 'No recent data', 'regime_signal': 0}
            _regime_cache['data'] = result
            _regime_cache['ts']   = time.time()
            return result

        mean_adx      = float(np.mean(last_adx))
        mean_bbw      = float(np.mean(last_bbw))
        mean_atr      = float(np.mean(last_atr))
        mean_ndipdi   = float(np.mean(last_ndipdi))
        mean_ndi      = float(np.mean(last_ndi))
        mean_pdi      = float(np.mean(last_pdi))

        # [V4-MOMENTUM] Trend FORMATION signals (lead) — replace old confirmation gates
        adx_5bar_avg  = float(np.mean(adx_5bar_ago))
        bbw_5bar_avg  = float(np.mean(bbw_5bar_ago))
        adx_velocity  = mean_adx - adx_5bar_avg              # +ve = trend forming
        bbw_expanding = (mean_bbw > bbw_5bar_avg * 1.05)     # vol breakout: +5%

        # Anti-chase: BTC 30min pump (each bar 5min × 6 bars = 30min)
        btc_arr = regime_data.get('BTC/USDC:USDC', {}).get('closes', None)
        if btc_arr is not None and len(btc_arr) > 6:
            btc_30m_pump = float(btc_arr[-1] / btc_arr[-7] - 1)
        is_overextended = btc_30m_pump > MAX_30MIN_PUMP_PCT

        # Light direction confirmation: PDI > NDI (don't enter while NDI dominates)
        # Threshold relaxed from -3 to -1 (was over-restrictive, blocked 80% of valid setups)
        pdi_dominant = (mean_ndipdi < -1.0)

        # Composite score kept for monitoring/logging only — NOT used as an entry gate
        def _norm(val, lo, hi):
            return float(np.clip((val - lo) / (hi - lo + 1e-9), 0.0, 1.0))
        all_adx_np = np.array(last_adx)
        adx_lo = safe_pct(all_adx_np, 10); adx_hi = safe_pct(all_adx_np, 90)
        bbw_lo = safe_pct(all_bbw,     10); bbw_hi = safe_pct(all_bbw,     90)
        adx_n  = _norm(mean_adx,     adx_lo, adx_hi)
        bbw_n  = _norm(mean_bbw,     bbw_lo, bbw_hi)
        di_n   = _norm(-mean_ndipdi, 0.0,    20.0)
        score  = 0.5 * adx_n + 0.3 * di_n + 0.2 * bbw_n

        is_highvol = (mean_atr > atr_hi)

        # Hard bear veto kept (cheap & safe). Threshold: >50% of 8 majors with 7d return < -3%.
        n_assets   = len(regime_data)
        bear_votes = sum(1 for sym, data in regime_data.items()
                         if len(data['ret']) > 0 and data['ret'][-1] < MACRO_BEAR_RTN_THR)
        bull_votes = sum(1 for sym, data in regime_data.items()
                         if len(data['ret']) > 0 and data['ret'][-1] > MACRO_BULL_RTN_THR)
        is_bear    = (bear_votes > n_assets // 2) and mean_adx > 30
        if bull_votes > n_assets // 2:
            is_bear = False

        # [V4-MOMENTUM] NEW DECISION LOGIC — trend formation, not confirmation
        # Emit +2 only if:
        #   1. Not high-vol panic (otherwise wide stops bleed)
        #   2. Not overextended (BTC hasn't already pumped >1.5% in last 30min)
        #   3. Trend FORMING — ADX rising OR volatility expanding
        #   4. PDI not strongly dominated by NDI (light direction filter)
        #   5. Not in confirmed bear (>50% majors down -3% over 7d AND macro ADX>30)
        regime_signal = 0
        _block_reason = []

        if is_highvol:
            _block_reason.append(f"L1-HighVol: ATR%={mean_atr:.4f} > {atr_hi:.4f}")
        elif is_overextended:
            _block_reason.append(f"L2-Overextended: BTC 30m={btc_30m_pump*100:+.2f}%"
                                 f" > {MAX_30MIN_PUMP_PCT*100:.1f}% (anti-chase)")
        elif is_bear:
            _block_reason.append(f"L3-Bear: bear_votes={bear_votes}/{n_assets} & ADX>{30}")
        elif not (adx_velocity > ADX_RISING_THR or bbw_expanding):
            _block_reason.append(
                f"L4-NoFormation: ΔADX(25m)={adx_velocity:+.2f} (need >+{ADX_RISING_THR}) | "
                f"BBW{'↑' if bbw_expanding else '→'}")
        elif not pdi_dominant:
            _block_reason.append(
                f"L5-NDIDominant: NDI-PDI={mean_ndipdi:+.2f} (need < -1.0)")
        else:
            regime_signal = +2

        if regime_signal == 0 and _block_reason:
            print(f"  🚧 信號封鎖原因: {' | '.join(_block_reason)}")

        if regime_signal == +2:
            signal = 1; brake = False; soft_brake = False; brake_reason = ""
        else:
            signal = 0; brake = False
            soft_brake   = True if is_highvol else False
            brake_reason = "高波動期" if is_highvol else "市場狀態中性"

        # [HL-17] Use USDC symbol keys for price extraction
        btc_p = regime_data.get('BTC/USDC:USDC', {}).get('closes', [0])[-1]
        eth_p = regime_data.get('ETH/USDC:USDC', {}).get('closes', [0])[-1]
        sol_p = regime_data.get('SOL/USDC:USDC', {}).get('closes', [0])[-1]

        signal_names = {0: "無信號", +2: "趨勢多頭"}
        status_text  = f"📊 市場狀態: {signal_names.get(regime_signal, '未知')}"
        if is_highvol: status_text += " ⚠️ 高波動期"
        if is_bear:    status_text += " 🐻 巨集觀熊市"

        log_status_to_csv({
            'btc_price':   round(btc_p, 2) if btc_p else 0,
            'adx':         round(mean_adx, 2),
            'signal_code': signal,
            'decision_text': status_text,
            'mean_ndi':    round(mean_ndi, 3),
            'mean_pdi':    round(mean_pdi, 3),
            'ndi_slope':   round(adx_velocity, 3),       # [V4] CSV: was ndi_slope, now ΔADX(25m)
            'pdi_slope':   round(btc_30m_pump * 100, 3), # [V4] CSV: was pdi_slope, now BTC 30m %
            'ndi_rising':  int(adx_velocity > ADX_RISING_THR),
            'pdi_rising':  int(bbw_expanding),
            'ndipdi':      round(mean_ndipdi, 3),
            'score':       round(score, 4),
            'ema_dir':     int(pdi_dominant),  # [V4] reused: 1 if PDI dominant
            'is_bear':     int(is_bear),
        })

        labels = [
            'BTC/ETH/SOL Price', '',
            'ATR%', 'HighVol', '',
            'Trend-Health Score (info)', 'ADX (mean)', 'BBW (mean)', '',
            'ΔADX 25m (formation)', 'BBW expanding', 'BTC 30m pump (anti-chase)', '',
            '-DI (mean)', '+DI (mean)', 'NDI-PDI', '',
            'Bear', 'bear_votes', 'bull_votes', '',
            'Signal', 'Decision',
        ]
        values = [
            f"{btc_p:.0f} / {eth_p:.0f} / {sol_p:.1f}", '',
            f"{mean_atr:.4f} (highvol_thr: {atr_hi:.4f})", f"{'Y ⚠️' if is_highvol else 'N'}", '',
            f"{score:.3f}  (ADX×0.5 + DI×0.3 + BBW×0.2) [INFO ONLY]",
            f"{mean_adx:.1f}", f"{mean_bbw:.4f}", '',
            f"{adx_velocity:+.2f}  (need > +{ADX_RISING_THR} for +2)",
            f"{'✅ ↑' if bbw_expanding else '→'}  (alt-trigger to ΔADX)",
            f"{btc_30m_pump*100:+.2f}%  (max {MAX_30MIN_PUMP_PCT*100:.1f}% else block)", '',
            f"{mean_ndi:.2f}", f"{mean_pdi:.2f}",
            f"{mean_ndipdi:+.2f}  (need <-1.0 for +2)", '',
            f"{'ON 🐻' if is_bear else 'OFF'}", f"{bear_votes}/{n_assets}", f"{bull_votes}/{n_assets}", '',
            f"{signal_names.get(regime_signal, '無信號')}", status_text,
        ]
        non_empty_labels = [l for l in labels if l != '']
        pad         = max(len(max(non_empty_labels, key=len)) + 4, 18)
        table_lines = [
            '' if lbl == '' else f"  {lbl:<{pad}}{val}"
            for lbl, val in zip(labels, values)
        ]
        utc_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        hdr_line = (f"🌐 市場狀態 V3-HL（{len(regime_data)} 個資產）[{utc_str}]"
                    + (" [SIM]" if SIMULATION_MODE else " [LIVE]"))
        sep_len  = max(len(hdr_line), max((len(L) for L in table_lines if L), default=0)) + 2
        sep_len  = max(sep_len, 60)
        print("-" * sep_len)
        print(hdr_line)
        print("-" * sep_len)
        for L in table_lines:
            print(L)
        print("-" * sep_len)

        # Telegram: market status notification (on signal change or hourly)
        global _last_market_signal, _last_market_notification_time
        current_time = time.time()
        if (TELEGRAM_ENABLED and ENABLE_TELEGRAM_SEND and
                (_last_market_signal != regime_signal or
                 current_time - _last_market_notification_time > 3600)):
            try:
                telegram_notifier.send_market_status({
                    'signal_names':    signal_names.get(regime_signal, '無信號'),
                    'mean_adx':        mean_adx,
                    'market_score':    score,
                    'is_highvol':      is_highvol,
                    'is_bear':         is_bear,
                    'btc_price':       btc_p,
                    'eth_price':       eth_p,
                    'sol_price':       sol_p,
                    'positions_count': len(positions),
                    'total_pnl':       sim_total_pnl if SIMULATION_MODE else 0,
                })
                _last_market_signal              = regime_signal
                _last_market_notification_time   = current_time
            except Exception as e:
                logger.warning(f"⚠️ Telegram市場狀態通知失敗: {e}")

        # [V4-MOMENTUM] ADX MEI (Momentum Exhaustion Index) REMOVED.
        # MEI was a band-aid for top-chasing entries: flagged "decelerating" ADX as a top.
        # With trend-FORMATION entries (ADX rising + anti-chase + tight SL), there's no
        # top to protect — if momentum dies, the 1.2×ATR SL handles it cheaply.

        result = {
            'signal':        signal,
            'brake':         brake,
            'soft_brake':    soft_brake,
            'brake_reason':  brake_reason,
            'regime_signal': regime_signal,
            'market_score':  score,        # trend-health 0-1 (info only, not a gate)
            'mean_adx':      mean_adx,
            'adx_velocity':  adx_velocity,  # ΔADX over 25m — primary formation signal
            'bbw_expanding': bbw_expanding,
            'btc_30m_pump':  btc_30m_pump,  # used by execute_live_long anti-chase guard
            'is_highvol':    is_highvol,
            'is_bear':       is_bear,
        }
        _regime_cache['data'] = result
        _regime_cache['ts']   = time.time()
        return result

    except Exception as e:
        logger.error(f"⚠️ 市場狀態檢測器故障: {e}")
        if _regime_cache['data'] is not None:
            logger.warning("⚠️ 使用上次緩存結果繼續運行")
            return _regime_cache['data']
        return {'signal': 0, 'brake': True, 'soft_brake': False,
                'brake_reason': f'API Error: {e}', 'regime_signal': 0}


# ==========================================
# 📡 [MODULE 8] Coin Scouter (Long: Strongest)
# ==========================================
def scouting_strong_coins(scouting_coins: int = 30) -> list:
    """
    Scan HL market for long candidates.
    [HL-18] Filter for ':USDC' perpetuals only.
    [HL-12] WHITELIST symbols use ':USDC' suffix.
    Two-stage filter to avoid low-liquidity chasing-top scenarios:
      Stage 1 – Absolute filters: WHITELIST, spread, MIN_VOLUME_USDC, MAX_SCOUT_CHANGE_PCT
      Stage 2 – Sort: top 2× pool by volume (liquidity), then re-rank by % change (momentum)
    """
    try:
        tickers = exchange.fetch_tickers()
        data    = []
        skipped_vol = skipped_top = 0
        for s, t in tickers.items():
            # [HL-18] USDC-margined perpetuals only
            if not s.endswith(':USDC'):
                continue
            if s not in WHITELIST or s in BLACKLIST:
                continue
            if t.get('percentage') is None:
                continue
            ask, bid = t.get('ask'), t.get('bid')
            if not (ask and bid and bid > 0):
                continue
            if (ask - bid) / bid >= 0.0010:   # spread too wide
                continue
            volume = t.get('quoteVolume', 0) or 0
            change = t['percentage']

            # Volume floor: avoid thin/illiquid coins
            if volume < MIN_VOLUME_USDC:
                skipped_vol += 1
                continue

            # Chasing-top guard: coin already pumped > MAX_SCOUT_CHANGE_PCT
            if change > MAX_SCOUT_CHANGE_PCT:
                skipped_top += 1
                continue

            data.append({'symbol': s, 'volume': volume, 'change': change})

        if skipped_vol or skipped_top:
            print(f"  🔍 Scout filters: -{skipped_vol} low-vol, -{skipped_top} chasing-top")

        df = pd.DataFrame(data)
        if df.empty:
            return []

        # Stage 2: top liquid pool → re-rank by momentum
        pool       = df.sort_values('volume', ascending=False).head(scouting_coins * 2)
        candidates = pool.sort_values('change', ascending=False).head(scouting_coins)
        return candidates['symbol'].tolist()

    except Exception as e:
        print(f"⚠️ Majors Scouting Error: {e}")
        return []


# ==========================================
# 🔍 [MODULE 9] Lee-Ready Flow Radar (Long)
# ==========================================
# [V4-MOMENTUM] check_flow_health() REMOVED — see manage_long_positions() for rationale.


def apply_lee_ready_long_logic(symbol: str) -> tuple:
    """
    Classify order flow direction using weighted Lee-Ready proxy.
    Returns: (net_flow, last_price, is_strong, acceleration, imbalance)
    """
    try:
        trades = exchange.fetch_trades(symbol, limit=200)
        if not trades:
            return 0, 0, False, 0, 0

        df                 = pd.DataFrame(trades)
        df['price_change'] = df['price'].diff()
        df['direction']    = np.where(df['price_change'] > 0, 1,
                                      np.where(df['price_change'] < 0, -1, 0))
        df['direction']    = df['direction'].replace(0, np.nan).ffill().fillna(0)
        avg_vol            = df['amount'].mean()
        df['weight']       = np.where(df['amount'] > avg_vol * 2, 2.0, 1.0)
        df['net_flow']     = df['direction'] * df['amount'] * df['price'] * df['weight']

        short_window_flow = df['net_flow'].tail(50).sum()
        acceleration      = df['net_flow'].tail(25).sum() - df['net_flow'].iloc[-50:-25].sum()

        try:
            ob        = exchange.fetch_order_book(symbol, limit=20)
            bids_vol  = sum(b[1] for b in ob['bids'])
            asks_vol  = sum(a[1] for a in ob['asks'])
            imbalance = ((bids_vol - asks_vol) / (bids_vol + asks_vol)
                         if (bids_vol + asks_vol) > 0 else 0)
        except Exception:
            imbalance = 0

        z_score   = 0
        is_strong = False
        flow_std  = df['net_flow'].std()
        if flow_std > 0:
            z_score = short_window_flow / (flow_std * np.sqrt(50))

        if short_window_flow > 0 and acceleration > 0 and imbalance > 0.15:
            is_strong = True
            print(f"🔥 {symbol} Long Sniper! Accel:{acceleration:.0f} | Imbalance:{imbalance:.2f}")
        elif z_score > NET_FLOW_SIGMA:
            is_strong = True
            print(f"📈 {symbol} Long Z-Score Validated: {z_score:.2f}")

        if is_strong and imbalance < -0.1:
            is_strong = False
            print(f"⚠️ {symbol} 假突破陷阱！取消做多！")

        return short_window_flow, df['price'].iloc[-1], is_strong, acceleration, imbalance

    except Exception as e:
        print(f"⚠️ LR Logic Error [{symbol}]: {e}")
        return 0, 0, False, 0, 0


# ==========================================
# 🛠️ [MODULE 10] PnL Settlement for Native Exits
# ==========================================
def process_native_exit_log(symbol: str, pos: dict, position_type: str = 'long') -> float:
    """
    Handle exchange-triggered exits (TP/SL hit, liquidation) and log PnL.
    [FIX-SIM] Sim mode: estimate from ticker, no private API call.
    [HL-6]    Live mode: replaced Bybit private_get_v5_position_closed_pnl
              with CCXT fetch_my_trades().
    [HL-13]   Log label updated to 'HL Native Exit / Liquidation'.
    """
    if SIMULATION_MODE:
        try:
            curr_p = exchange.fetch_ticker(symbol)['last']
        except Exception:
            curr_p = pos['entry_price']
        real_pnl = round((curr_p - pos['entry_price']) * pos['amount'], 4)
        log_to_csv({'symbol': symbol, 'action': 'NATIVE_EXIT', 'price': curr_p,
                    'amount': pos['amount'], 'reason': 'Sim Native TP/SL',
                    'realized_pnl': real_pnl})
        return real_pnl

    real_exit_price = pos['entry_price']
    real_pnl        = 0.0
    try:
        # [HL-6] Reconstruct PnL from recent fills via CCXT fetch_my_trades()
        recent_trades = exchange.fetch_my_trades(symbol, limit=5)
        close_fills   = [t for t in recent_trades if t.get('side', '') == 'sell']
        if close_fills:
            last_fill       = close_fills[-1]
            real_exit_price = float(last_fill.get('price', pos['entry_price']))
            fill_fee        = float(last_fill.get('fee', {}).get('cost', 0) or 0)
            fill_amount     = float(last_fill.get('amount', pos['amount']))
            real_pnl        = round(
                (real_exit_price - pos['entry_price']) * fill_amount - fill_fee, 4
            )
        else:
            raise ValueError("No closing fills found in fetch_my_trades")
    except Exception as e:
        logger.debug(f"⚠️ {symbol} 獲取真實 PnL 失敗，備用估算: {e}")
        try:
            curr_p          = exchange.fetch_ticker(symbol)['last']
            real_exit_price = curr_p
            fee_estimate    = curr_p * pos['amount'] * 0.0007
            real_pnl        = round((curr_p - pos['entry_price']) * pos['amount'] - fee_estimate, 4)
        except Exception:
            pass

    # [HL-13] Updated label
    log_to_csv({'symbol': symbol, 'action': 'NATIVE_EXIT', 'price': real_exit_price,
                'amount': pos['amount'], 'reason': 'HL Native Exit / Liquidation',
                'realized_pnl': real_pnl})
    return real_pnl


# ==========================================
# 🛡️ [MODULE 11] Startup Position Sync
# ==========================================
def sync_positions_on_startup() -> None:
    """
    On restart, adopt any open long positions from exchange into bot memory.
    [FIX-SIM] Sim mode: skip (initial positions are always zero).
    [HL-19]   Removed params={'category': 'linear'}.
    [HL-20]   Use HL field names: 'entryPx' in info dict, 'szi' for size.
    """
    if SIMULATION_MODE:
        print("🔵 [SIM] 跳過倉位同步（模擬模式，初始持倉為零）")
        return

    print("🔄 正在同步 Hyperliquid 現有多單...")
    try:
        # [HL-19] No category param; pass user to identify account
        live_positions_raw = exchange.fetch_positions(None, {'user': API_KEY})
        recovered_count    = 0

        for p in live_positions_raw:
            symbol    = p['symbol']
            side      = p.get('side', '').lower()
            info_side = p.get('info', {}).get('side', '').lower()

            if not (side in ['long', 'buy'] or info_side in ['buy', 'long']):
                continue

            # [HL-20] HL uses 'entryPx' in info dict; CCXT normalizes to 'entryPrice'
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
                atr = entry_price * 0.01

            sl_p = float(p.get('stopLoss') or 0)
            tp_p = float(p.get('takeProfit') or 0)
            if sl_p == 0:
                sl_p = float(exchange.price_to_precision(symbol, entry_price - SL_ATR_MULT * atr))
            if tp_p == 0:
                tp_p = float(exchange.price_to_precision(symbol, entry_price + TP_ATR_MULT * atr))

            is_be = (sl_p > entry_price and sl_p > 0)
            positions[symbol] = {
                'amount':      amount,
                'entry_price': entry_price,
                'tp_price':    tp_p,
                'sl_price':    sl_p,
                'is_breakeven':          is_be,
                'atr':                   atr,
                'max_pnl_pct':           0.0,
                'entry_time':            time.time(),
                'side':                  'long',
            }
            recovered_count += 1
            print(f"✅ 成功尋回孤兒多單: {symbol} | 入場價: {entry_price:.4f} | 已保本: {is_be}")

        print(f"🔄 同步完成！共尋回 {recovered_count} 個多倉。")
    except Exception as e:
        logger.error(f"❌ 啟動同步失敗: {e}")


# ==========================================
# 🛡️ [MODULE 12] Position Manager (Long-Only, HL)
# ==========================================
def manage_long_positions(regime: dict = None) -> None:
    """
    Main position management loop (long-only).
    [HL-8]  Trail SL managed locally only; no exchange API call for SL updates.
            (HL has no trading_stop equivalent; relies on local logic.)
    [HL-14] Bybit "10006" string check → ccxt.RateLimitExceeded exception.
    [HL-19] fetch_positions() without Bybit category params.
    [HL-20] Position field mapping for HL (entryPx, szi).
    [HL-23] Short-specific code removed (long-only bot).
    """
    try:
        live_positions_raw = get_live_positions_cached()
        live_symbols = {
            p['symbol']: p for p in live_positions_raw
            if float(p.get('contracts', 0) or
                     p.get('info', {}).get('szi', 0) or 0) > 0
        }

        # ── Step 1: Auto-adopt orphan long positions ──
        for s, p in live_symbols.items():
            if s in positions:
                continue
            side      = p.get('side', '').lower()
            info_side = p.get('info', {}).get('side', '').lower()
            if not (side in ['long', 'buy'] or info_side in ['buy', 'long']):
                continue

            # [HL-20] HL field names
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

            # [HL-20] HL createdTime may be in ms
            raw_ts       = p.get('createdTime') or p.get('info', {}).get('time')
            real_entry_t = (float(raw_ts) / 1000.0) if raw_ts else time.time()

            sl_p = float(p.get('stopLoss') or 0)
            tp_p = float(p.get('takeProfit') or 0)
            if sl_p == 0:
                sl_p = float(exchange.price_to_precision(s, entry_p - SL_ATR_MULT * atr))
            if tp_p == 0:
                tp_p = float(exchange.price_to_precision(s, entry_p + TP_ATR_MULT * atr))

            is_be = (sl_p > entry_p and sl_p > 0)
            positions[s] = {
                'amount':      amt,
                'entry_price': entry_p,
                'tp_price':    tp_p,
                'sl_price':    sl_p,
                'is_breakeven':          is_be,
                'atr':                   atr,
                'max_pnl_pct':           0.0,
                'entry_time':            real_entry_t,
                'side':                  'long',
            }
            print(f"🚨 [自癒] 發現並接管孤兒多單: {s} | 入場:{entry_p:.4f}")

        # ── Step 2: Detect positions closed by exchange ──
        for s in list(positions.keys()):
            if s not in live_symbols:
                print(f"🧹 {'[SIM] ' if SIMULATION_MODE else ''}倉位已平: {s}")
                real_pnl = process_native_exit_log(s, positions[s], 'long')
                _safe_influx(_influx_write_trade,
                             symbol=s, action='NATIVE_EXIT',
                             price=positions[s].get('entry_price', 0),
                             amount=positions[s].get('amount', 0),
                             realized_pnl=real_pnl, sim_mode=SIMULATION_MODE)
                cancel_all_hl(s)
                handle_trade_result(s, real_pnl, is_sl_exit=True)
                del positions[s]
                if SIMULATION_MODE and s in sim_positions:
                    del sim_positions[s]
                continue

        if not positions:
            return

        current_prices = fetch_tickers_for_positions(list(positions.keys()))

        for s in list(positions.keys()):
            try:
                curr_p = current_prices.get(s)
                if curr_p is None:
                    logger.warning(f"⚠️ {s} 無現價，跳過")
                    continue

                pos          = positions[s]
                rs0          = (regime or {}).get('regime_signal', 0)
                pnl_pct      = (curr_p - pos['entry_price']) / pos['entry_price']
                coin_vol_pct = pos['atr'] / pos['entry_price']
                sl_updated   = False

                if 'max_pnl_pct' not in pos:
                    pos['max_pnl_pct'] = pnl_pct
                pos['max_pnl_pct'] = max(pos['max_pnl_pct'], pnl_pct)

                _safe_influx(_influx_write_position,
                             symbol=s, side='long',
                             entry_price=pos['entry_price'],
                             current_price=curr_p,
                             pnl_pct=pnl_pct,
                             unrealized_pnl=pnl_pct * pos['entry_price'] * pos['amount'],
                             time_held_secs=time.time() - pos.get('entry_time', time.time()),
                             sim_mode=SIMULATION_MODE)

                # ── Regime 轉向收 SL（持倉 >= 30 分鐘 + regime 已轉中性/空頭）──
                _YOUNG_POS_AGE = 1800
                if (not pos['is_breakeven']
                        and time.time() - pos.get('entry_time', time.time()) >= _YOUNG_POS_AGE
                        and rs0 <= 0
                        and pos.get('entry_regime_signal', 1) > 0):
                    _tight_sl = curr_p - (0.8 * pos['atr'])
                    if _tight_sl > pos['sl_price']:
                        pos['sl_price'] = _tight_sl
                        sl_updated      = True
                        _held_min       = (time.time() - pos.get('entry_time', time.time())) / 60
                        print(f"🔶 {s} Regime轉向→收緊SL: {pos['sl_price']:.4f} "
                              f"(Regime={rs0}, 持倉{_held_min:.1f}min)")

                # ── Breakeven push (profit > 2.0 × ATR%) ──
                if not pos['is_breakeven'] and pnl_pct > (coin_vol_pct * 2.0):
                    pos['sl_price']     = pos['entry_price'] * 1.002
                    pos['is_breakeven'] = True
                    sl_updated          = True

                # [V4-MOMENTUM] Single-stage trailing stop (was 5-stage with regime/decel branches).
                # 5-stage trail had no data-driven justification; the only thing that matters
                # for momentum trades is "give back at most 1×ATR of unrealised peak".
                if pos['is_breakeven']:
                    trail_sl = curr_p - (1.0 * pos['atr'])
                    if trail_sl > pos['sl_price']:
                        if (trail_sl - pos['sl_price']) / pos['sl_price'] > 0.0005:
                            sl_updated      = True
                            pos['sl_price'] = trail_sl

                # [HL-8] Trail SL is managed locally only.
                # No exchange API call needed (HL has no trading_stop equivalent).
                if sl_updated:
                    logger.debug(f"📐 {s} Trail SL → {pos['sl_price']:.4f} (pnl={pnl_pct*100:.2f}%)")
                    if SIMULATION_MODE and s in sim_positions:
                        sim_positions[s]['sl_price'] = pos['sl_price']

                # [V4-MOMENTUM] Hard timeout — 30min, no extension.
                # Old code: 90min first timeout + 180min extension if Regime+ADX still healthy.
                # Problem: extending bad trades is opportunity-cost expensive AND the
                # extension condition (Regime>0 + ADX>=25) is itself a top-chasing filter.
                # Better: cut losses fast, redeploy capital quickly.
                exit_reason = None
                time_held   = time.time() - pos.get('entry_time', time.time())

                if time_held > TIMEOUT_SECONDS and pnl_pct < 0.005:
                    if pnl_pct < TIMEOUT_LOSS_FLOOR:
                        exit_reason = "Timeout (Loss Floor)"
                        print(f"⏱️ {s} 30min Timeout: PnL {pnl_pct*100:+.2f}%，出場")
                    else:
                        exit_reason = "Timeout (Stalled)"
                        print(f"⏱️ {s} 30min Timeout: 未達 +0.5%，釋放資金")

                # [V4-MOMENTUM] check_flow_health REMOVED.
                # Old: "Flow Deceleration" set deceleration_detected=True which then triggered
                # tighter trail SL → exit too early. "Flow Reversal" tried to predict dumps
                # but on 60min horizon WR is 44.9% (vs 30min 15.5%) — most "reversals" recover.
                # Trust the trail SL (1×ATR behind peak) to handle real reversals cheaply.

                # ── Local TP/SL Check ──
                if not exit_reason:
                    if curr_p >= pos['tp_price']:
                        exit_reason = "TP (Long IOC Exit)"
                    elif curr_p <= pos['sl_price']:
                        exit_reason = ("Trail SL (Long IOC Exit)"
                                       if pos['is_breakeven'] else "SL (Long IOC Exit)")

                # ── Execute Exit ──
                if exit_reason:
                    print(f"⚔️ {exit_reason} | {s} | {time_held/60:.1f}分 | "
                          f"MaxPnL:{pos['max_pnl_pct']*100:.2f}% | 現:{pnl_pct*100:.2f}%"
                          + (" [SIM]" if SIMULATION_MODE else ""))

                    if SIMULATION_MODE:
                        ioc_price = get_3_layer_avg_price(s, 'bids') or curr_p
                        ioc_pnl   = sim_close_long(s, pos['amount'], ioc_price)
                        if s in sim_positions:
                            del sim_positions[s]
                    else:
                        ioc_price = get_3_layer_avg_price(s, 'bids') or curr_p
                        try:
                            # [HL-15] Removed positionIdx (Bybit hedge-mode only)
                            exchange.create_order(s, 'limit', 'sell', pos['amount'], ioc_price,
                                                  {'timeInForce': 'IOC', 'reduceOnly': True})
                        except Exception:
                            exchange.create_market_sell_order(s, pos['amount'], {'reduceOnly': True})
                        entry_fee = pos['entry_price'] * pos['amount'] * FEE_RATE
                        exit_fee  = ioc_price * pos['amount'] * FEE_RATE
                        ioc_pnl   = round(
                            (ioc_price - pos['entry_price']) * pos['amount'] - entry_fee - exit_fee, 4
                        )
                        _positions_cache['ts'] = 0  # invalidate cache

                    log_to_csv({'symbol': s, 'action': 'LONG_EXIT', 'price': curr_p,
                                'amount': pos['amount'], 'reason': exit_reason,
                                'realized_pnl': ioc_pnl})
                    _safe_influx(_influx_write_trade,
                                 symbol=s, action='LONG_EXIT',
                                 price=curr_p, amount=pos['amount'],
                                 realized_pnl=ioc_pnl, sim_mode=SIMULATION_MODE)

                    if TELEGRAM_ENABLED and ENABLE_TELEGRAM_SEND:
                        try:
                            telegram_notifier.send_trade_alert(
                                symbol=s, action='LONG_EXIT', price=curr_p,
                                amount=pos['amount'], reason=exit_reason, pnl=ioc_pnl
                            )
                        except Exception as te:
                            logger.warning(f"⚠️ Telegram通知失敗: {te}")

                    cancel_all_hl(s)
                    _is_sl = 'SL' in exit_reason and 'TP' not in exit_reason
                    handle_trade_result(s, ioc_pnl, is_sl_exit=_is_sl)
                    del positions[s]

            except Exception as inner_e:
                # [HL-14] Replaced Bybit "10006" string check
                if isinstance(inner_e, ccxt.RateLimitExceeded):
                    logger.warning("⚠️ Rate limit in position loop, sleeping 10s")
                    time.sleep(10)
                else:
                    logger.error(f"❌ {s} 持倉管理錯誤: {inner_e}")

    except ccxt.RateLimitExceeded:
        logger.warning("⏳ manage_long_positions Rate Limit，等待 10s")
        time.sleep(10)
    except Exception as e:
        logger.error(f"❌ manage_long_positions 外層錯誤: {e}")


# ==========================================
# 🚀 [MODULE 13] Entry Executor (Long, HL)
# ==========================================
def execute_live_long(symbol: str, net_flow: float, current_price: float,
                      is_strong: bool, atr, is_volatile: bool,
                      regime: dict = None, position_multiplier: float = 1.0) -> None:
    """
    Size and execute a long entry via IOC limit order.
    [HL-7]  No exchange-native TP/SL call. TP/SL enforced locally via
            manage_long_positions(). (HL has no trading_stop equivalent.)
    [HL-9]  set_leverage: generalized error handling (removed Bybit codes).
    [HL-15] IOC params: removed 'positionIdx': 0.
    [HL-16] Leverage errors: generalized (removed 110043/110026 guards).
    [FIX-SIM] Sim mode: uses sim_open_long() instead of exchange order.
    Args:
        position_multiplier: sizing scale factor from Sensor B + MEI.
    """
    _r                = regime or {}
    regime_signal_tag = _r.get('regime_signal', 0)
    adx_tag           = round(_r.get('mean_adx', 0), 2)
    score_tag         = round(_r.get('market_score', 0), 4)

    if symbol in cooldown_tracker:
        if time.time() < cooldown_tracker[symbol]:
            return
        else:
            del cooldown_tracker[symbol]

    if atr is None or atr == 0 or current_price == 0:
        return
    if not (is_strong and is_volatile and symbol not in positions):
        return

    # [V4-MOMENTUM] Regime-level anti-chase: if BTC has already pumped > MAX_30MIN_PUMP_PCT,
    # we are too late. The regime detector already enforces this, but we re-check here
    # because by the time Lee-Ready triggers per-symbol, BTC may have moved further.
    _btc_pump = (regime or {}).get('btc_30m_pump', 0.0)
    if _btc_pump > MAX_30MIN_PUMP_PCT:
        logger.debug(f"⛔ [Anti-chase] {symbol} BTC 30m={_btc_pump*100:+.2f}% > "
                     f"{MAX_30MIN_PUMP_PCT*100:.1f}%，跳過")
        return

    # ── [V4] Symbol-level formation gate (ADX rising + breakout + symbol not extended) ──
    _trend = check_symbol_trend(symbol)
    if not _trend.get('is_long_ok'):
        logger.debug(f"⛔ [Symbol-gate] {symbol} formation 未達: {_trend.get('reason', 'n/a')}")
        return
    print(f"✅ [Symbol-gate] {symbol} formation: {_trend.get('reason', 'n/a')}")

    # Tier 2 auto-scale
    if symbol in TIER2_SET:
        position_multiplier *= TIER2_SIZE_MULTIPLIER
        print(f"🔸 {symbol} TIER2 縮倉: position_multiplier={position_multiplier:.2f}")

    # ── DUPCHECK: exchange live position double-confirm ──
    if not SIMULATION_MODE:
        try:
            live_pos  = get_live_positions_cached()
            live_syms = {
                p['symbol'] for p in live_pos
                if float(p.get('contracts', 0) or
                         p.get('info', {}).get('szi', 0) or 0) > 0
            }
            if symbol in live_syms:
                logger.warning(f"⚠️ [DUPCHECK] {symbol} 交易所已有倉位，拒絕重複開倉")
                for p in live_pos:
                    if p['symbol'] == symbol:
                        entry_p = float(p.get('entryPrice') or
                                        p.get('info', {}).get('entryPx', 0) or 0)
                        amt     = float(p.get('contracts', 0) or
                                        p.get('info', {}).get('szi', 0) or 0)
                        atr_v, _ = get_market_metrics(symbol)
                        if not atr_v: atr_v = entry_p * 0.01
                        sl_p = (float(p.get('stopLoss') or 0) or
                                float(exchange.price_to_precision(symbol, entry_p - SL_ATR_MULT * atr_v)))
                        tp_p = (float(p.get('takeProfit') or 0) or
                                float(exchange.price_to_precision(symbol, entry_p + TP_ATR_MULT * atr_v)))
                        positions[symbol] = {
                            'amount': amt, 'entry_price': entry_p,
                            'tp_price': tp_p, 'sl_price': sl_p,
                            'is_breakeven': sl_p > entry_p, 'atr': atr_v,
                            'max_pnl_pct': 0.0, 'entry_time': time.time(), 'side': 'long',
                        }
                        print(f"🔄 [DUPCHECK] 已補回 {symbol} 至本地 positions dict")
                        break
                return
        except Exception as e:
            logger.warning(f"⚠️ [DUPCHECK] {symbol} 查詢失敗，繼續執行: {e}")

    # Hard position cap
    if len(positions) >= MAX_CONCURRENT_POSITIONS:
        logger.debug(f"⛔ {symbol} 倉位已達上限 {MAX_CONCURRENT_POSITIONS}")
        return

    # [V4-MOMENTUM] Cascade SL protection REMOVED — see strategy parameters block.
    cancel_all_hl(symbol)
    actual_bal = get_live_usdc_balance()
    eff_bal    = min(WORKING_CAPITAL, actual_bal)

    trade_val = min(
        (eff_bal * RISK_PER_TRADE * position_multiplier) / ((SL_ATR_MULT * atr) / current_price),
        eff_bal * MAX_LEVERAGE * 0.95 * position_multiplier,
        MAX_NOTIONAL_PER_TRADE * position_multiplier
    )

    amount = float(exchange.amount_to_precision(symbol, trade_val / current_price))
    if amount < exchange.markets[symbol]['limits']['amount'].get('min', 0):
        return

    ioc_p = get_3_layer_avg_price(symbol, 'asks') or current_price
    if amount * ioc_p < MIN_NOTIONAL:
        return

    # [HL-9] Leverage — generalized error handling (no Bybit-specific codes)
    if not SIMULATION_MODE:
        try:
            exchange.set_leverage(int(MAX_LEVERAGE), symbol)
        except Exception as e:
            # [HL-16] Removed Bybit "110043"/"110026" guards
            logger.warning(f"⚠️ {symbol} 槓桿設置異常 (繼續嘗試入場): {e}")

    # ── Execute order ──
    if SIMULATION_MODE:
        actual_amount, actual_price = sim_open_long(symbol, amount, ioc_p)
        if actual_amount == 0:
            print(f"⏩ [SIM] {symbol} 餘額不足，跳過。")
            return
    else:
        try:
            # [HL-15] Removed positionIdx (Bybit hedge-mode specific)
            order = exchange.create_order(symbol, 'limit', 'buy', amount, ioc_p,
                                          {'timeInForce': 'IOC'})
            time.sleep(1)
            actual_price, actual_amount = ioc_p, 0.0

            try:
                od = exchange.fetch_order(order['id'], symbol, params={"acknowledged": True})
                actual_price  = float(od.get('average') or od.get('price') or ioc_p)
                actual_amount = float(od.get('filled', 0))
            except Exception as e:
                logger.warning(f"⚠️ {symbol} 訂單確認失敗，備用持倉同步: {e}")
                time.sleep(0.5)
                # Fallback: scan live positions (no category param for HL)
                for p in exchange.fetch_positions():
                    if (p['symbol'] == symbol and
                            float(p.get('contracts', 0) or
                                  p.get('info', {}).get('szi', 0) or 0) > 0):
                        actual_amount = float(p.get('contracts', 0) or
                                              p.get('info', {}).get('szi', 0) or 0)
                        actual_price  = float(p.get('entryPrice') or
                                              p.get('info', {}).get('entryPx', ioc_p) or ioc_p)
                        break

            if actual_amount == 0:
                print(f"⏩ {symbol} IOC 未成交，撤單退出。")
                cancel_all_hl(symbol)
                return

        except ccxt.RateLimitExceeded:
            # [HL-14] Generic rate-limit handler
            logger.warning(f"⏳ {symbol} 入場遭遇 Rate Limit，等待後重試")
            time.sleep(5)
            return
        except Exception as e:
            logger.error(f"❌ {symbol} 做多執行失敗: {e}")
            return

    # ── Calculate TP/SL ──
    tp_p = float(exchange.price_to_precision(symbol, actual_price + TP_ATR_MULT * atr))
    sl_p = float(exchange.price_to_precision(symbol, actual_price - SL_ATR_MULT * atr))

    if (tp_p - actual_price) / actual_price < 0.003:
        print(f"🟡 放棄做多 [{symbol}]: 利潤空間太細！"
              + (" [SIM 退還本金]" if SIMULATION_MODE else ""))
        if SIMULATION_MODE:
            global sim_balance
            refund = actual_amount * actual_price
            sim_balance += refund
            logger.info(f"🔵 [SIM] {symbol} 退還本金 {refund:.4f}（利潤空間太細）")
        else:
            try:
                exchange.create_market_sell_order(symbol, actual_amount, {'reduceOnly': True})
            except Exception as e:
                logger.error(f"❌ {symbol} 緊急平倉失敗: {e}")
            cancel_all_hl(symbol)
        return

    # [HL-7] No exchange-native TP/SL call here.
    #         Bybit used: private_post_v5_position_trading_stop(...)
    #         HL: TP/SL enforced entirely via local logic in manage_long_positions().
    if SIMULATION_MODE:
        print(f"🔵 [SIM] {symbol} 虛擬 TP:{tp_p} | SL:{sl_p}")
        sim_positions[symbol] = {
            'amount':      actual_amount,
            'entry_price': actual_price,
            'tp_price':    tp_p,
            'sl_price':    sl_p,
            'entry_time':  time.time(),
            'side':        'long',
        }
    else:
        print(f"✅ {symbol} 本地止盈止損已記錄 | TP:{tp_p:.4f} | SL:{sl_p:.4f}")
        _positions_cache['ts'] = 0  # invalidate cache

    # ── Update local position dict ──
    positions[symbol] = {
        'amount':      actual_amount,
        'entry_price': actual_price,
        'tp_price':    tp_p,
        'sl_price':    sl_p,
        'is_breakeven':          False,
        'atr':                   atr,
        'max_pnl_pct':           0.0,
        'entry_time':            time.time(),
        'side':                  'long',
        'entry_regime_signal':   regime_signal_tag,
    }
    cooldown_tracker[symbol] = time.time() + 480
    save_dynamic_blacklist()

    log_to_csv({
        'symbol':            symbol,
        'action':            'LONG_ENTRY',
        'price':             actual_price,
        'amount':            actual_amount,
        'trade_value':       round(actual_amount * actual_price, 2),
        'atr':               round(atr, 4),
        'net_flow':          round(net_flow, 2),
        'tp_price':          tp_p,
        'sl_price':          sl_p,
        'actual_balance':    round(actual_bal, 2),
        'effective_balance': eff_bal,
        'regime_signal':     regime_signal_tag,
        'mean_adx':          adx_tag,
        'market_score':      score_tag,
    })
    _safe_influx(_influx_write_trade,
                 symbol=symbol, action='LONG_ENTRY',
                 price=actual_price, amount=actual_amount,
                 atr=atr, net_flow=net_flow,
                 tp_price=tp_p, sl_price=sl_p,
                 regime_signal=regime_signal_tag, mean_adx=adx_tag,
                 market_score=score_tag, sim_mode=SIMULATION_MODE)
    # [HL-2] Log USDC denomination
    print(f"📈 {'[SIM] ' if SIMULATION_MODE else ''}[入貨做多] {symbol} "
          f"@ {actual_price:.4f} USDC | 數量:{actual_amount}")

    if TELEGRAM_ENABLED and ENABLE_TELEGRAM_SEND:
        try:
            telegram_notifier.send_trade_alert(
                symbol=symbol, action='LONG_ENTRY',
                price=actual_price, amount=actual_amount,
                reason=f"趨勢多頭 | ADX:{adx_tag} | Score:{score_tag}"
            )
        except Exception as e:
            logger.warning(f"⚠️ Telegram通知發送失敗: {e}")


# ==========================================
# 🚀 [MODULE 14] Main Event Loop
# ==========================================
def main() -> None:
    mode_label = "🔵 SIMULATION" if SIMULATION_MODE else "🟢 LIVE TRADE"
    print("=" * 60)
    print(f"🚀 AI 實戰 V4 Trend FORMATION Long [Hyperliquid] [{mode_label}] 啟動")
    print(f"   SL={SL_ATR_MULT}×ATR | TP={TP_ATR_MULT}×ATR (R:R={TP_ATR_MULT/SL_ATR_MULT:.1f}:1)")
    print(f"   Anti-chase: 跳過若 BTC 30min > {MAX_30MIN_PUMP_PCT*100:.1f}%")
    print(f"   入場: ΔADX(25m)>+{ADX_RISING_THR} OR BBW expanding | Lee-Ready flow + breakout")
    print(f"   Timeout={TIMEOUT_SECONDS//60}min | Trail=1×ATR | ActiveSignals={ACTIVE_LONG_SIGNALS}")
    print(f"   Fee: taker={FEE_RATE*100:.4f}% / maker={FEE_RATE_MAKER*100:.4f}%")
    print(f"   Regime緩存={REGIME_CACHE_TTL}s | ATR緩存={ATR_CACHE_TTL}s | Pos緩存={POSITIONS_CACHE_TTL}s")
    if not SIMULATION_MODE:
        print("=" * 60)
        print("  ⚠️  SL 重要提示: HL 無交易所原生 SL，止損完全依賴本進程")
        print("  ⚠️  請確保已啟動 start.sh 守護腳本 或 systemd/pm2 服務")
        print(f"  💓 Heartbeat 寫入: {HEARTBEAT_FILE}  (每 {HEARTBEAT_INTERVAL}s 更新)")
    print("=" * 60)

    load_dynamic_blacklist()
    sync_positions_on_startup()

    last_scout_time   = 0
    target_coins      = []
    _last_brake_state = None
    _sim_report_ts    = time.time()
    global _last_heartbeat_ts

    while True:
        try:
            regime = get_btc_regime_v3_fast()
            manage_long_positions(regime)

            # Heartbeat: write every HEARTBEAT_INTERVAL so guardian detects hangs
            _now = time.time()
            if _now - _last_heartbeat_ts >= HEARTBEAT_INTERVAL:
                write_heartbeat()
                _last_heartbeat_ts = _now

            # InfluxDB: write regime status every cycle
            _safe_influx(_influx_write_regime,
                         regime_signal=regime.get('regime_signal', 0),
                         mean_adx=regime.get('mean_adx', 0.0),
                         market_score=regime.get('market_score', 0.0),
                         adx_mei=regime.get('adx_velocity', 0.0),  # [V4] reused field: ADX velocity
                         brake=bool(regime.get('brake', False)),
                         soft_brake=bool(regime.get('soft_brake', False)),
                         sim_mode=SIMULATION_MODE)

            # InfluxDB: write balance every cycle
            if SIMULATION_MODE:
                _safe_influx(_influx_write_balance,
                             balance=sim_balance, equity=sim_equity,
                             total_pnl=sim_total_pnl, sim_mode=True)
            else:
                try:
                    _safe_influx(_influx_write_balance,
                                 balance=get_live_usdc_balance(), sim_mode=False)
                except Exception:
                    pass

            curr_t = time.time()

            # Sim mode: periodic performance report every 5 min
            if SIMULATION_MODE and (curr_t - _sim_report_ts > 300):
                sim_report()
                _sim_report_ts = curr_t

            if curr_t - last_scout_time > SCOUTING_INTERVAL:

                target_coins    = scouting_strong_coins(30)
                last_scout_time = curr_t

                _current_state = ('HARD' if regime.get('brake') else
                                  'SOFT' if regime.get('soft_brake') else 'GREEN')
                regime_signal  = regime.get('regime_signal', 0)
                is_long_signal = regime_signal in ACTIVE_LONG_SIGNALS

                _SIGNAL_LABEL = {0: "中性", +2: "趨勢多頭✅"}
                if _current_state != _last_brake_state:
                    print(f"📡 Regime: {_SIGNAL_LABEL.get(regime_signal, '未知')} | "
                          f"啟用多頭訊號: {ACTIVE_LONG_SIGNALS}")

                if is_long_signal:
                    # [V4-MOMENTUM] Position multiplier simplified:
                    #   - Sensor B (consecutive-regime ramp 0.25→1.0)  REMOVED
                    #     Reason: scaled UP after trend ran for 12 cycles = late-entry magnifier.
                    #   - MEI top protection gate (block when adx_mei < -2)  REMOVED
                    #     Reason: was a band-aid for top-chasing entries; with formation entries
                    #     + tight 1.2×ATR SL, "top protection" is structurally unnecessary.
                    #   - SOFT-DAILY (× 0.5 when 1D not bullish)  REMOVED
                    #     Reason: 1D consensus has ~0 correlation with fwd 30m return on majors.
                    #   - Cascade SL freeze  REMOVED
                    #     Reason: cascade SL was caused by top-chasing entries; new trend-formation
                    #     entries with 1.2×ATR SL have decorrelated SL events.
                    position_multiplier = 1.0
                    mean_adx        = regime.get('mean_adx', 0)
                    adx_velocity    = regime.get('adx_velocity', 0)
                    btc_30m_pump    = regime.get('btc_30m_pump', 0)
                    print(f"🟢 [V4] 趨勢多頭 ADX={mean_adx:.1f} ΔADX25m={adx_velocity:+.2f}"
                          f" BTC30m={btc_30m_pump*100:+.2f}%"
                          f" {'[SIM]' if SIMULATION_MODE else ''}")

                    if len(positions) >= MAX_CONCURRENT_POSITIONS:
                        print(f"⛔ 倉位已達上限 {MAX_CONCURRENT_POSITIONS}，跳過本輪掃描")
                        _last_brake_state = _current_state
                        time.sleep(POSITION_CHECK_INTERVAL)
                        continue

                    # Lee-Ready flow scan: collect signals, prioritise sniper (flow>0 + accel>0 + imbal>+0.15).
                    _prescan = []
                    for s in target_coins:
                        try:
                            flow, last_p, is_strong, accel, imbal = apply_lee_ready_long_logic(s)
                            if last_p > 0:
                                _is_sniper = (flow > 0 and accel > 0 and imbal > 0.15)
                                _prescan.append((s, flow, last_p, is_strong, accel, _is_sniper))
                        except Exception:
                            pass
                        time.sleep(0.3)

                    _prescan.sort(key=lambda x: (not x[5], -x[4]))
                    _sniper_coins = [r[0] for r in _prescan if r[5]]
                    if _sniper_coins:
                        print(f"🎯 Sniper 優先入場排序: {_sniper_coins}")

                    for s, flow, last_p, is_strong, accel, _ in _prescan:
                        try:
                            atr, is_v = get_market_metrics(s)
                            execute_live_long(s, flow, last_p, is_strong,
                                             atr, is_v, regime=regime,
                                             position_multiplier=position_multiplier)
                        except Exception:
                            continue

                else:
                    if _current_state != _last_brake_state:
                        print(f"🚦 {regime.get('brake_reason', '市場中性')}，暫停入場")

                _last_brake_state = _current_state
                bal_str = (f"SimBal:{sim_balance:.2f} PnL:{sim_total_pnl:+.4f}"
                           if SIMULATION_MODE else f"餘額:{get_live_usdc_balance():.2f} USDC")
                print(f"⏳ {'[SIM]' if SIMULATION_MODE else ''} 多單巡邏 | "
                      f"持倉:{list(positions.keys())} | {bal_str}")

            time.sleep(POSITION_CHECK_INTERVAL)

        except KeyboardInterrupt:
            print("\n👋 手動終止。")
            if SIMULATION_MODE:
                sim_report()
            else:
                print(f"餘額:{get_live_usdc_balance():.2f} USDC | 持倉:{list(positions.keys())}")
            sys.exit(0)

        except ccxt.RateLimitExceeded:
            # [HL-14] Generic rate-limit handler (replaces Bybit "10006" string check)
            logger.warning("⏳ 主迴圈 Rate Limit，等待 10s...")
            time.sleep(10)

        except Exception as e:
            logger.error(f"❌ 主迴圈錯誤: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
