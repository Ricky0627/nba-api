import pandas as pd
import os
import requests
import random
from nba_api.stats.endpoints import teamgamelogs
import warnings

# 忽略 NBA API 的警告訊息
warnings.filterwarnings("ignore", category=UserWarning, module="nba_api")

# ===========================
# ⚙️ 測試設定區 (雙重驗證版)
# ===========================
TEST_SEASON = '2025-26'
SEASON_TYPE = 'Regular Season'
MEASURE_TYPE = 'Base'
TIMEOUT_SECONDS = 30  

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15'
]

def get_headers():
    return {
        'Host': 'stats.nba.com',
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://www.nba.com/',
        'Origin': 'https://www.nba.com',
        'x-nba-stats-origin': 'stats',
        'x-nba-stats-token': 'true',
    }

def test_and_fetch():
    proxy_url = os.environ.get('PROXY_URL')
    
    if not proxy_url:
        print("❌ 找不到 PROXY_URL 環境變數！請確認是否已在 GitHub Secrets 設定。")
        return pd.DataFrame()

    # 🔥 關鍵修復：直接將 Proxy 寫入系統環境變數，這樣底層的 requests 就能最完美地處理它
    os.environ['HTTP_PROXY'] = proxy_url
    os.environ['HTTPS_PROXY'] = proxy_url

    # ==========================================
    # 步驟一：先測試 Proxy 到底有沒有通
    # ==========================================
    print(f"🔍 步驟一：測試 Webshare Proxy 連線是否正常...")
    try:
        # 連到一個專門用來測試 IP 的網站
        res = requests.get('https://httpbin.org/ip', timeout=15)
        print(f"✅ Proxy 測試成功！目前對外偽裝的 IP 是: {res.json().get('origin')}")
    except Exception as e:
        print(f"❌ Proxy 測試失敗，連不上 Webshare: {e}")
        print("👉 診斷建議：請檢查 GitHub Secrets 裡的 PROXY_URL 格式是否為：http://帳號:密碼@IP:Port (開頭一定要有 http://)")
        return pd.DataFrame()

    # ==========================================
    # 步驟二：測試 NBA API
    # ==========================================
    print(f"\n📡 步驟二：使用私人 Proxy 連線 NBA API 獲取 {TEST_SEASON} 數據...")
    try:
        # 注意：我們不再傳入 proxy=proxy_url，因為上面已經設定了全域環境變數
        logs = teamgamelogs.TeamGameLogs(
            season_nullable=TEST_SEASON,
            season_type_nullable=SEASON_TYPE,
            measure_type_player_game_logs_nullable=MEASURE_TYPE,
            headers=get_headers(),
            timeout=TIMEOUT_SECONDS
        )
        df = logs.get_data_frames()[0]
        print("✅ 成功突圍！取得 NBA 數據！")
        return df
    except Exception as e:
        print(f"❌ NBA API 抓取失敗: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    print("🚀 啟動 NBA 數據爬蟲 (Proxy 雙重診斷版)")
    df = test_and_fetch()
    
    if not df.empty:
        print("\n📊 成功解析數據！資料總筆數:", len(df))
        target_team = 'POR'
        team_df = df[df['TEAM_ABBREVIATION'] == target_team].copy()
        
        if not team_df.empty:
            print(f"\n🏀 顯示 {target_team} 最近的 5 場比賽紀錄：")
            display_cols = ['GAME_DATE', 'MATCHUP', 'WL', 'PTS', 'REB', 'AST']
            print(team_df[display_cols].head(5).to_markdown(index=False))
        else:
            print(f"⚠️ 找不到 {target_team} 的資料，顯示前 5 筆：\n", df.head(5))
