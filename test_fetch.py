import pandas as pd
import time
import random
import requests
from nba_api.stats.endpoints import teamgamelogs
import warnings

# 忽略 NBA API 的警告訊息
warnings.filterwarnings("ignore", category=UserWarning, module="nba_api")

# ===========================
# ⚙️ 測試設定區
# ===========================
TEST_SEASON = '2025-26'
SEASON_TYPE = 'Regular Season'
MEASURE_TYPE = 'Base'
TIMEOUT_SECONDS = 15  # 縮短超時時間，免費 Proxy 連不上就趕快換下一個
MAX_PROXY_TRIES = 15  # 最多測試 15 個不同的 Proxy

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0'
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

def get_free_proxies():
    """從開源庫自動抓取最新的免費 Proxy 列表"""
    print("🔍 正在從網路獲取免費 Proxy 列表...")
    try:
        # 使用一個時常更新的 GitHub 免費 Proxy 清單
        url = "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt"
        res = requests.get(url, timeout=10)
        proxies = res.text.strip().split('\n')
        # 過濾出格式正確的 IP:Port
        valid_proxies = [p for p in proxies if ':' in p]
        print(f"✅ 成功獲取 {len(valid_proxies)} 個 Proxy！將隨機抽取 {MAX_PROXY_TRIES} 個進行突圍測試。")
        return random.sample(valid_proxies, MAX_PROXY_TRIES)
    except Exception as e:
        print(f"❌ 獲取 Proxy 清單失敗: {e}")
        return []

def fetch_with_proxy(season, season_type, measure_type):
    proxies = get_free_proxies()
    if not proxies:
        return pd.DataFrame()

    print(f"\n📡 開始嘗試連線 NBA API 獲取 {season} 數據...")
    
    for i, proxy_ip in enumerate(proxies, 1):
        # 組合出正確的 proxy 格式
        proxy_url = f"http://{proxy_ip}"
        print(f"[{i}/{MAX_PROXY_TRIES}] 🔄 嘗試替身 IP: {proxy_url} ...", end=" ")
        
        try:
            # 這裡把 proxy 傳給 nba_api
            logs = teamgamelogs.TeamGameLogs(
                season_nullable=season,
                season_type_nullable=season_type,
                measure_type_player_game_logs_nullable=measure_type,
                headers=get_headers(),
                timeout=TIMEOUT_SECONDS,
                proxy=proxy_url
            )
            print("✅ 成功突圍！取得數據！")
            return logs.get_data_frames()[0]
            
        except Exception as e:
            # 簡化錯誤訊息，因為免費 proxy 失敗是家常便飯
            error_msg = str(e).split(":")[-1].strip()[:30]
            print(f"❌ 失敗 ({error_msg})，換下一個")
            
    return pd.DataFrame()

if __name__ == "__main__":
    print("🚀 啟動 NBA 數據爬蟲 (自動 Proxy 突圍版)")
    
    df = fetch_with_proxy(TEST_SEASON, SEASON_TYPE, MEASURE_TYPE)
    
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
        print("\n❌ 突圍失敗：測試的 Proxy 全部失效，請再觸發一次 Action 試試看。")
