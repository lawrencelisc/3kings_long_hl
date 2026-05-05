import ccxt
import yaml
import time

# 讀取配置
with open('../config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)
hl_config = config['hyperliquid']

exchange = ccxt.hyperliquid({
    'walletAddress': hl_config['wallet_address'],
    'private_key': hl_config['private_key'],
    'enableRateLimit': True
})


def get_hl_radar_metrics(symbol, levels=5):
    """計算 HL 專屬的雷達指標"""
    ob = exchange.fetch_order_book(symbol, limit=levels)

    # 1. 計算買賣盤總量 (前 5 層)
    total_bid_size = sum([b[1] for b in ob['bids']])
    total_ask_size = sum([a[1] for a in ob['asks']])

    # 2. 計算 Imbalance
    imbalance = (total_bid_size - total_ask_size) / (total_bid_size + total_ask_size)

    # 3. 計算 Spread (價差 %)
    best_bid = ob['bids'][0][0]
    best_ask = ob['asks'][0][0]
    spread_pct = (best_ask - best_bid) / best_bid * 100

    return {
        'price': best_bid,
        'imbalance': imbalance,
        'spread_pct': spread_pct,
        'bid_v': total_bid_size,
        'ask_v': total_ask_size
    }


def main_loop():
    symbol = 'BTC/USDC:USDC'
    print(f"📡 3Kings HL 高速雷達已啟動... 監控對象: {symbol}")

    while True:
        try:
            metrics = get_hl_radar_metrics(symbol)

            # 戰略判斷邏輯
            signal = "🟡 STANDBY"
            if metrics['imbalance'] > 0.4:
                signal = "🟢 BULLISH (Strong Support)"
            elif metrics['imbalance'] < -0.4:
                signal = "🔴 BEARISH (Heavy Resistance)"

            print(f"[{time.strftime('%H:%M:%S')}] 價: {metrics['price']} | "
                  f"Imbalance: {metrics['imbalance']:.2f} | "
                  f"Spread: {metrics['spread_pct']:.4f}% | 狀態: {signal}")

            # 高頻掃描：每 1 秒掃描一次 (HL API 支援極速請求)
            time.sleep(1)

        except Exception as e:
            print(f"❌ 雷達故障: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main_loop()