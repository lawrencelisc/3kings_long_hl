import ccxt
import yaml
import time

# 1. 讀取安全裝甲
with open('../config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)
hl_config = config['hyperliquid']

# 2. 初始化交易所
exchange = ccxt.hyperliquid({
    'walletAddress': hl_config['wallet_address'],
    'private_key': hl_config['private_key'],
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})


def run_fire_test():
    symbol = 'BTC/USDC:USDC'
    qty = 0.001  # 測試用極小量 (約 $70 USD nominal)

    try:
        print(f"🚀 演習開始：目標 {symbol} | 槓桿 10x")

        # A. 設定槓桿 (Hyperliquid 必須先設定)
        exchange.set_leverage(10, symbol)
        print("✅ 槓桿已設定為 10x")

        # ---------------------------------------------------------
        # 第一階段：Taker IOC (即時掃貨)
        # ---------------------------------------------------------
        print("\n🔥 階段 1：執行 Taker IOC Long...")
        # 為了確保 Taker 能成交，我們會以稍微高於現價的價格發出 IOC
        ticker = exchange.fetch_ticker(symbol)
        taker_price = ticker['ask'] * 1.001  # 墊高 0.1% 確保吃單

        taker_order = exchange.create_order(
            symbol=symbol,
            type='limit',
            side='buy',
            amount=qty,
            price=taker_price,
            params={'timeInForce': 'IOC'}  # 核心：即時成交，否則取消
        )
        print(f"✅ Taker IOC 發出！訂單 ID: {taker_order['id']} | 狀態: {taker_order['status']}")

        time.sleep(2)  # 稍等兩秒，觀察戰果

        # ---------------------------------------------------------
        # 第二階段：3 層 Orderbook Average Maker (掛單)
        # ---------------------------------------------------------
        print("\n🔥 階段 2：執行 3 層平均價 Maker Long...")
        ob = exchange.fetch_order_book(symbol, limit=5)

        # 計算前三層 Bid 的平均價
        # $$ \text{Average Price} = \frac{Bid_1 + Bid_2 + Bid_3}{3} $$
        b1, b2, b3 = ob['bids'][0][0], ob['bids'][1][0], ob['bids'][2][0]
        avg_maker_price = (b1 + b2 + b3) / 3

        print(f"📊 3 層 Bid 價格：{b1}, {b2}, {b3} | 平均：{avg_maker_price:.2f}")

        maker_order = exchange.create_order(
            symbol=symbol,
            type='limit',
            side='buy',
            amount=qty,
            price=round(avg_maker_price, 1),
            params={'postOnly': True}  # 核心：確保只做 Maker，否則取消
        )
        print(f"✅ Maker 掛單成功！價格: {avg_maker_price:.2f} | 狀態: {maker_order['status']}")

    except Exception as e:
        print(f"❌ 演習出錯: {e}")


def check_battle_results():
    symbol = 'BTC/USDC:USDC'
    print("\n🕵️‍♂️ 正在掃描戰場回報...")

    # 1. 檢查目前持倉 (睇下 IOC 食咗幾多)
    pos = exchange.fetch_positions([symbol])
    for p in pos:
        if p['symbol'] == symbol:
            print(f"🔹 目前持倉數量: {p['contracts']} | 入場均價: {p['entryPrice']}")

    # 2. 檢查掛單 (睇下 Maker 單仲喺唔喺度)
    open_orders = exchange.fetch_open_orders(symbol)
    if open_orders:
        for o in open_orders:
            print(f"🔸 偵測到掛單: {o['side']} {o['amount']} @ {o['price']} (ID: {o['id']})")
    else:
        print("🔸 目前無任何掛單 (Maker 可能已成交或被取消)")


if __name__ == "__main__":
    # run_fire_test()
    check_battle_results()