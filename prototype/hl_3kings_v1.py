import ccxt
import yaml
import time
import logging

# ==========================================
# 0. 基礎配置與日誌設定
# ==========================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

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
QUANTITY = 0.001  # 每次作戰單位
IMBALANCE_THRESHOLD_LONG = 0.8  # 買盤極端強勁門檻
IMBALANCE_THRESHOLD_EXIT = -0.7  # 賣盤湧現逃頂門檻


# ==========================================
# 2. 核心戰術模組
# ==========================================

def get_leeready_metrics(symbol, levels=10):
    """Lee-Ready 大單加權失衡度計算"""
    try:
        ob = exchange.fetch_order_book(symbol, limit=levels)
        # 距離加權係數：越靠近成交價權重越高
        weights = [1.0, 0.8, 0.6, 0.4, 0.2, 0.1, 0.05, 0.05, 0.05, 0.05]

        weighted_bid_v = 0
        weighted_ask_v = 0

        # 掃描買盤 (Bids)
        for i in range(min(len(ob['bids']), levels)):
            price, size = ob['bids'][i]
            # 巨鯨偵測：單筆超過 1 BTC 權重翻倍
            multiplier = 2.0 if size > 1.0 else 1.0
            weighted_bid_v += size * weights[i] * multiplier

        # 掃描賣盤 (Asks)
        for i in range(min(len(ob['asks']), levels)):
            price, size = ob['asks'][i]
            multiplier = 2.0 if size > 1.0 else 1.0
            weighted_ask_v += size * weights[i] * multiplier

        # 計算加權失衡度
        # $$ \text{LR Imbalance} = \frac{V_{bid} - V_{ask}}{V_{bid} + V_{ask}} $$
        lr_imbalance = (weighted_bid_v - weighted_ask_v) / (weighted_bid_v + weighted_ask_v)
        return lr_imbalance, ob['bids'][0][0], ob['asks'][0][0]
    except Exception as e:
        logging.error(f"雷達掃描失敗: {e}")
        return 0, 0, 0


def get_current_position():
    """獲取目前持倉數量"""
    try:
        positions = exchange.fetch_positions([SYMBOL])
        for p in positions:
            if p['symbol'] == SYMBOL:
                return float(p['contracts'])
        return 0.0
    except Exception as e:
        logging.error(f"持倉讀取失敗: {e}")
        return 0.0


def execute_taker_ioc(side, amount):
    """執行 Taker IOC (快速進場/撤軍)"""
    try:
        ticker = exchange.fetch_ticker(SYMBOL)
        # 為了確保 Taker 成交，買入墊高價格，賣出壓低價格
        price = ticker['ask'] * 1.002 if side == 'buy' else ticker['bid'] * 0.998
        order = exchange.create_order(
            symbol=SYMBOL,
            type='limit',
            side=side,
            amount=amount,
            price=price,
            params={'timeInForce': 'IOC'}
        )
        logging.info(f"⚡ Taker IOC {side} 執行 | ID: {order['id']}")
        return order
    except Exception as e:
        logging.error(f"Taker 執行失敗: {e}")


def execute_3layer_maker(side, amount):
    """執行 3 層平均價 Maker 掛單"""
    try:
        ob = exchange.fetch_order_book(SYMBOL, limit=5)
        if side == 'buy':
            prices = [ob['bids'][0][0], ob['bids'][1][0], ob['bids'][2][0]]
        else:
            prices = [ob['asks'][0][0], ob['asks'][1][0], ob['asks'][2][0]]

        avg_price = sum(prices) / 3
        order = exchange.create_order(
            symbol=SYMBOL,
            type='limit',
            side=side,
            amount=amount,
            price=round(avg_price, 1),
            params={'postOnly': True}
        )
        logging.info(f"🧱 Maker {side} 掛單成功 | 價格: {avg_price:.1f}")
        return order
    except Exception as e:
        logging.error(f"Maker 執行失敗: {e}")


# ==========================================
# 3. 主循環：三皇高頻決策系統
# ==========================================

def main():
    logging.info("🛡️ 3Kings Hyperliquid 重裝版啟動")
    exchange.set_leverage(LEVERAGE, SYMBOL)

    while True:
        try:
            # 1. 雷達掃描指標
            lr_imb, bid_p, ask_p = get_leeready_metrics(SYMBOL)
            current_pos = get_current_position()

            logging.info(f"📡 [監控] 價: {bid_p} | LR-Imbalance: {lr_imb:.2f} | 持倉: {current_pos}")

            # 2. 決策邏輯
            # A. 撤軍邏輯：持有長倉且賣盤極端湧現
            if current_pos > 0 and lr_imb < IMBALANCE_THRESHOLD_EXIT:
                logging.warning("⚠️ 偵測到強力壓盤，執行 Taker IOC 撤軍止盈！")
                execute_taker_ioc('sell', current_pos)

            # B. 入場邏輯：無持倉且買盤強力支撐
            elif current_pos == 0 and lr_imb > IMBALANCE_THRESHOLD_LONG:
                logging.info("🟢 買盤支撐強勁，執行 3 層 Maker 混合入場...")
                # 第一部分：先用 Taker IOC 搶佔 50% 倉位
                execute_taker_ioc('buy', QUANTITY * 0.5)
                # 第二部分：剩下的 50% 用 Maker 平均價掛單
                execute_3layer_maker('buy', QUANTITY * 0.5)

            # C. 高頻刷新頻率 (1 秒一次)
            time.sleep(1)

        except KeyboardInterrupt:
            logging.info("停火指令已下達，系統安全關閉。")
            break
        except Exception as e:
            logging.error(f"主循環發生未預期錯誤: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()