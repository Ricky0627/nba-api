import pandas as pd
import time
import random
from nba_api.stats.endpoints import teamgamelogs
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError
from urllib3.exceptions import ProtocolError
import warnings

# 忽略 NBA API 的警告訊息
warnings.filterwarnings("ignore", category=UserWarning, module="nba_api")

# ===========================
# ⚙️ 測試設定區
# ===========================
TEST_SEASON = '2025-26'
SEASON_TYPE = 'Regular Season'
MEASURE_TYPE = 'Base'
TIMEOUT_SECONDS = 60  
MAX_RETRIES = 5        
RETRY_DELAY = 3        

# === 🔥 終極防護：真實瀏覽器偽裝 ===
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
]

def get_headers():
    """每次請求隨機產生一組正常的瀏覽器標頭，避免被防火牆阻擋"""
    return {
        'Host': 'stats.nba.com',
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://www.nba.com/',
        'Origin': 'https://www.nba.com',
        'x-nba-stats-origin': 'stats',
        'x-nba-stats-token': 'true',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
    }

def fetch_with_retry(season, season_type, measure_type):
    """帶有智慧退避與偽裝的抓取函數 (測試版)"""
    print(f"📡 開始嘗試連線 NBA API 獲取 {season} {season_type} 數據...")
    for attempt in range(MAX_RETRIES):
        try:
            logs = teamgamelogs.TeamGameLogs(
                season_nullable=season,
                season_type_nullable=season_type,
                measure_type_player_game_logs_nullable=measure_type,
                headers=get_headers(),
                timeout=TIMEOUT_SECONDS
            )
            print("✅ 成功取得 API 回應！")
            return logs.get_data_frames()[0]
            
        except (ReadTimeout, ConnectTimeout, ConnectionError, ProtocolError) as e:
            wait_time = RETRY_DELAY + (attempt * 3) + random.uniform(1.5, 3.5)
            error_brief = str(e).split("',")[-1].strip(" )\"'")[:30] 
            print(f"⚠️ 伺服器無回應，第 {attempt + 1} 次重試，等待 {wait_time:.1f} 秒... [{error_brief}]")
            time.sleep(wait_time)
            
        except Exception as e:
            if "no data" in str(e).lower() or "timeout" in str(e).lower():
                break
            print(f"❌ API 發生未知錯誤: {e}")
            break
            
    return pd.DataFrame()

if __name__ == "__main__":
    print("🚀 啟動 NBA 數據爬蟲 GitHub Actions 測試版")
    
    df = fetch_with_retry(TEST_SEASON, SEASON_TYPE, MEASURE_TYPE)
    
    if not df.empty:
        print("\n📊 成功解析數據！資料總筆數:", len(df))
        
        # 篩選特定球隊來確認欄位與資料正確性
        target_team = 'POR'
        team_df = df[df['TEAM_ABBREVIATION'] == target_team].copy()
        
        if not team_df.empty:
            print(f"\n🏀 顯示 {target_team} 最近的 5 場比賽紀錄：")
            # 只顯示幾個關鍵欄位方便在 Log 閱讀
            display_cols = ['GAME_DATE', 'MATCHUP', 'WL', 'PTS', 'REB', 'AST']
            print(team_df[display_cols].head(5).to_markdown(index=False))
        else:
            print(f"⚠️ 找不到 {target_team} 的比賽數據。")
            print("顯示前 5 筆原始資料：\n", df.head(5))
            
        # 輸出成小檔案確認寫入權限 (GitHub Actions Artifacts 備用)
        df.head(100).to_csv('test_output.csv', index=False)
        print("\n💾 已儲存前 100 筆資料至 test_output.csv")
    else:
        print("\n❌ 測試失敗：無法取得任何數據，可能已被阻擋或賽季無資料。")
