import ccxt
import yaml
import time

# ==========================================
# 0. 讀取 YAML 配置文件 (安全裝甲)
# ==========================================
try:
    with open('../config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
except FileNotFoundError:
    print("❌ 找不到 config/config.yaml 檔案，請確保資料夾與檔案存在！")
    exit()

hl_config = config.get('hyperliquid', {})

# 提取「老闆地址」與「打工仔私鑰」
wallet_address = hl_config.get('wallet_address', '')
private_key = hl_config.get('private_key', '')
print(f'wallet_address: {wallet_address}')
print(f'private_key: {private_key}')

if not wallet_address or not private_key:
    print("❌ 讀取配置失敗：請檢查 config.yaml 內是否已填妥 wallet_address 及 private_key")
    exit()

# ==========================================
# 1. 初始化 Hyperliquid 交易所
# ==========================================
exchange = ccxt.hyperliquid({
    'walletAddress': wallet_address,
    'privateKey': private_key,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap',  # 指定預設為永續合約
    }
})


def test_hyperliquid_connection():
    print("🚀 啟動 Hyperliquid 先遣探測機 (終極搜查版)...")
    print("-" * 50)

    try:
        # 1. 確認探測機到底查緊邊個地址！
        print(f"🕵️‍♂️ 探測機鎖定之老闆地址: [{exchange.walletAddress}]")

        # 2. 強制搜查【合約 (Perp/Swap)】金庫
        print("\n🏦 正在搜查 [合約] 金庫...")
        perp_balance = exchange.fetch_balance({'type': 'swap'})
        print(f"👉 合約可用 USDC: {perp_balance.get('USDC', {}).get('free', 0.0)}")

        # 3. 強制搜查【現貨 (Spot)】金庫 (破案關鍵！)
        print("\n🏦 正在搜查 [現貨] 金庫...")
        spot_balance = exchange.fetch_balance({'type': 'spot'})
        # 現貨嘅 USDC 可能喺 API 叫 USDC，或者直接喺 info 入面
        spot_usdc = spot_balance.get('USDC', {}).get('free', '找不到')
        print(f"👉 現貨可用 USDC: {spot_usdc}")
        print(f"📦 [現貨底層原始數據]: {spot_balance.get('info', {})}")

        print("-" * 50)
        print("🎉 搜查完畢！")

    except Exception as e:
        print(f"❌ 搜查失敗: {e}")

if __name__ == "__main__":
    test_hyperliquid_connection()