import ccxt
import yaml
import time
import logging

# ==========================================
# 0. 基礎配置與初始資金
# ==========================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# 填入您最初存入的金額，用來計算盈虧
STARTING_CAPITAL = 158.38

try:
    with open('../config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    hl_config = config['hyperliquid']
except Exception as e:
    logging.error(f"無法讀取配置文件: {e}")
    exit()

# ==========================================
# 1. 初始化 Hyperliquid 引擎
# ==========================================
exchange = ccxt.hyperliquid({
    'walletAddress': hl_config['wallet_address'],
    'privateKey': hl_config['private_key'],
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})

SYMBOL = 'BTC/USDC:USDC'
LEVERAGE = 10
QUANTITY = 0.001

# --- 戰術參數優化 (沉穩型) ---
IMBALANCE_THRESHOLD_LONG = 0.95  # 從 0.8 提高到 0.95 (只打絕對把握的單)
IMBALANCE_THRESHOLD_EXIT = -0.9  # 從 -0.7 降到 -0.9 (給行情更多呼吸空間，減少手續費磨損)


# ==========================================
# 2. 核心功能模組
# ==========================================

def check_pnl():
    """最強硬路徑版：如果呢段都讀唔到 157.02，我就要切腹謝罪"""
    try:
        # 直接調用 Hyperliquid 專有的私有方法來獲取帳戶狀態
        user_state = exchange.private_post_info({'type': 'userState', 'user': exchange.walletAddress})

        # 深入提取 Unified Account 的 accountValue
        acc_val = user_state['marginSummary']['accountValue']

        current_value = float(acc_val)
        pnl = current_value - STARTING_CAPITAL
        pnl_pct = (pnl / STARTING_CAPITAL) * 100
        status = "盈利 💰" if pnl >= 0 else "虧損 ⚠️"
        return current_value, pnl, pnl_pct, status
    except Exception as e:
        # 萬一失敗，印出錯誤代碼方便我哋 debug
        return STARTING_CAPITAL, -0.0001, 0.0, f"Error: {str(e)[:10]}"


def get_leeready_metrics(symbol, levels=10):
    """Lee-Ready 大單加權雷達"""
    try:
        ob = exchange.fetch_order_book(symbol, limit=levels)
        weights = [1.0, 0.8, 0.6, 0.4, 0.2, 0.1, 0.05, 0.05, 0.05, 0.05]
        weighted_bid_v = 0
        weighted_ask_v = 0

        for i in range(min(len(ob['bids']), levels)):
            price, size = ob['bids'][i]
            # 巨鯨加權邏輯
            multiplier = 2.0 if size > 1.0 else 1.0
            weighted_bid_v += size * weights[i] * multiplier

        for i in range(min(len(ob['asks']), levels)):
            price, size = ob['asks'][i]
            multiplier = 2.0 if size > 1.0 else 1.0
            weighted_ask_v += size * weights[i] * multiplier

        lr_imbalance = (weighted_bid_v - weighted_ask_v) / (weighted_bid_v + weighted_ask_v)
        return lr_imbalance, ob['bids'][0][0]
    except Exception as e:
        return 0, 0


def execute_taker_ioc(side, amount):
    """執行快速 Taker 單"""
    try:
        ticker = exchange.fetch_ticker(SYMBOL)
        price = ticker['ask'] * 1.002 if side == 'buy' else ticker['bid'] * 0.998
        exchange.create_order(SYMBOL, 'limit', side, amount, price, {'timeInForce': 'IOC'})
        logging.info(f"⚡ 執行 Taker {side} (IOC)")
    except Exception as e:
        logging.error(f"Taker 執行出錯: {e}")


# ==========================================
# 3. 主循環：三皇高頻決策
# ==========================================

def main():
    logging.info("🛡️ 3Kings V2 獲利監控版啟動 | 目標：穩健獲利")
    exchange.set_leverage(LEVERAGE, SYMBOL)

    while True:
        try:
            # 1. 獲取數據與當前盈虧
            lr_imb, price = get_leeready_metrics(SYMBOL)
            current_val, pnl, pnl_pct, status = check_pnl()

            # 2. 獲取持倉
            pos = exchange.fetch_positions([SYMBOL])
            current_pos = 0.0
            for p in pos:
                if p['symbol'] == SYMBOL:
                    current_pos = float(p['contracts'])

            # 3. 戰報顯示 (指揮官最關心的部分)
            # $$ \text{Total PnL} = \text{Current Value} - 158.38 $$
            print(f"[{time.strftime('%H:%M:%S')}] 價: {price} | LR-Imb: {lr_imb:.2f} | 持倉: {current_pos}")
            print(f"💰 帳戶總值: {current_val:.2f} USDC | 盈虧: {pnl:.4f} ({pnl_pct:.2f}%) | 狀態: {status}")
            print("-" * 60)

            # 4. 決策邏輯
            # A. 撤軍：持倉且賣壓極端
            if current_pos > 0 and lr_imb < IMBALANCE_THRESHOLD_EXIT:
                logging.warning("🔴 偵測到強大壓盤，快速撤軍止盈！")
                execute_taker_ioc('sell', current_pos)

            # B. 入場：無倉且買盤極端
            elif current_pos == 0 and lr_imb > IMBALANCE_THRESHOLD_LONG:
                logging.info("🟢 買盤力道驚人，閃電入場！")
                execute_taker_ioc('buy', QUANTITY)

            time.sleep(1)

        except Exception as e:
            logging.error(f"系統故障: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()