import ccxt
import yaml
import time
import logging

# ==========================================
# 0. 基礎配置
# ==========================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
STARTING_CAPITAL = 158.38

try:
    with open('../config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    hl_config = config['hyperliquid']
except Exception as e:
    logging.error(f"無法讀取配置文件: {e}")
    exit()

exchange = ccxt.hyperliquid({
    'walletAddress': hl_config['wallet_address'],
    'privateKey': hl_config['private_key'],
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})

# --- 核心戰略參數 (防磨損優化) ---
SYMBOL = 'BTC/USDC:USDC'
LEVERAGE = 10  # 撤回 10x 降低風險
QUANTITY = 0.001
IMBALANCE_THRESHOLD_LONG = 0.95  # 提高門檻，只打絕對強勢行情
IMBALANCE_THRESHOLD_EXIT = -0.9  # 給予最大呼吸空間，減少被騙炮
COOL_DOWN_SECONDS = 300  # 平倉後冷卻 300 秒 (5 分鐘)

# 全域狀態
last_exit_time = 0


# ==========================================
# 1. 強化功能模組
# ==========================================

def check_pnl_truth():
    """真相版 PnL：深入 Unified Account 的 marginSummary"""
    try:
        # 直接抓取底層 userState 數據
        state = exchange.private_post_info({'type': 'userState', 'user': exchange.walletAddress})
        acc_val = float(state['marginSummary']['accountValue'])

        pnl = acc_val - STARTING_CAPITAL
        pnl_pct = (pnl / STARTING_CAPITAL) * 100
        status = "盈利 💰" if pnl >= 0 else "虧損 ⚠️"
        return acc_val, pnl, pnl_pct, status
    except Exception as e:
        return STARTING_CAPITAL, -0.0001, 0.0, f"Syncing ({str(e)[:5]})"


def get_leeready_metrics(symbol, levels=10):
    """加權失衡度計算"""
    try:
        ob = exchange.fetch_order_book(symbol, limit=levels)
        weights = [1.0, 0.8, 0.6, 0.4, 0.2, 0.1, 0.05, 0.05, 0.05, 0.05]
        wb, wa = 0, 0
        for i in range(min(len(ob['bids']), levels)):
            wb += ob['bids'][i][1] * weights[i] * (2.0 if ob['bids'][i][1] > 1.0 else 1.0)
        for i in range(min(len(ob['asks']), levels)):
            wa += ob['asks'][i][1] * weights[i] * (2.0 if ob['asks'][i][1] > 1.0 else 1.0)
        return (wb - wa) / (wb + wa), ob['bids'][0][0]
    except:
        return 0, 0


def execute_taker_ioc(side, amount):
    """執行 Taker IOC"""
    try:
        ticker = exchange.fetch_ticker(SYMBOL)
        price = ticker['ask'] * 1.002 if side == 'buy' else ticker['bid'] * 0.998
        exchange.create_order(SYMBOL, 'limit', side, amount, price, {'timeInForce': 'IOC'})
        logging.info(f"⚡ [ACTION] Taker {side} (IOC) 執行成功")
    except Exception as e:
        logging.error(f"執行失敗: {e}")


# ==========================================
# 2. 主循環：冷卻機制與決策
# ==========================================

def main():
    global last_exit_time
    logging.info(f"🛡️ 3Kings V3 啟動 | 冷卻期: {COOL_DOWN_SECONDS}s | 槓桿: {LEVERAGE}x")
    exchange.set_leverage(LEVERAGE, SYMBOL)

    while True:
        try:
            lr_imb, price = get_leeready_metrics(SYMBOL)
            current_val, pnl, pnl_pct, status = check_pnl_truth()

            # 獲取持倉
            pos = exchange.fetch_positions([SYMBOL])
            current_pos = float(pos[0]['contracts']) if pos and pos[0]['symbol'] == SYMBOL else 0.0

            # 計算冷卻剩餘時間
            time_since_exit = time.time() - last_exit_time
            cool_down_remaining = max(0, int(COOL_DOWN_SECONDS - time_since_exit))

            # 顯示戰報
            print(f"[{time.strftime('%H:%M:%S')}] 價: {price} | LR-Imb: {lr_imb:.2f} | 持倉: {current_pos}")
            print(f"💰 實際淨值: {current_val:.2f} USDC | PnL: {pnl:.4f} ({pnl_pct:.2f}%) | 狀態: {status}")
            if cool_down_remaining > 0 and current_pos == 0:
                print(f"⏳ 戰鬥冷卻中... 剩餘 {cool_down_remaining} 秒")
            print("-" * 60)

            # 決策邏輯
            # A. 撤軍：持倉且信號轉空
            if current_pos > 0 and lr_imb < IMBALANCE_THRESHOLD_EXIT:
                logging.warning("🔴 強力壓盤！執行 Taker IOC 撤軍！")
                execute_taker_ioc('sell', current_pos)
                last_exit_time = time.time()  # 紀錄退出時間

            # B. 入場：無倉、買盤極端、且已過冷卻期
            elif current_pos == 0 and lr_imb > IMBALANCE_THRESHOLD_LONG and cool_down_remaining == 0:
                logging.info("🟢 買盤強力支撐且冷卻完畢，閃電進場！")
                execute_taker_ioc('buy', QUANTITY)

            time.sleep(1)

        except Exception as e:
            logging.error(f"系統故障: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()