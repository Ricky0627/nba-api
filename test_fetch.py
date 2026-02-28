import pandas as pd
import os
import random
from nba_api.stats.endpoints import teamgamelogs
import warnings

# 忽略 NBA API 的警告訊息
warnings.filterwarnings("ignore", category=UserWarning, module="nba_api")

# ===========================
# ⚙️ 測試設定區 (專屬 Proxy 版)
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

def fetch_with_private_proxy():
    # 🔑 從 GitHub Secrets (環境變數) 讀取你的專屬 Proxy
    proxy_url = os.environ.get('PROXY_URL')
    
    if not proxy_url:
        print("❌ 找不到 PROXY_URL 環境變數！請確認是否已在 GitHub Secrets 設定。")
        return pd.DataFrame()

    print(f"📡 使用私人專屬 Proxy 連線 NBA API 獲取 {TEST_SEASON} 數據...")
    
    try:
        logs = teamgamelogs.TeamGameLogs(
            season_nullable=TEST_SEASON,
            season_type_nullable=SEASON_TYPE,
            measure_type_player_game_logs_nullable=MEASURE_TYPE,
            headers=get_headers(),
            timeout=TIMEOUT_SECONDS,
            proxy=proxy_url
        )
        df = logs.get_data_frames()[0]
        print("✅ 成功突圍！取得數據！")
        return df
    except Exception as e:
        print(f"❌ 失敗: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    print("🚀 啟動 NBA 數據爬蟲 (私人專屬 Proxy 版)")
    df = fetch_with_private_proxy()
    
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
    else:
        print("\n❌ 抓取失敗，請檢查 Proxy 設定。")
