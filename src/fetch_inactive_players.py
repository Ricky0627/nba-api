import pandas as pd
import sqlite3
import time
import random
import concurrent.futures
import warnings
import os
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError
from urllib3.exceptions import ProtocolError

# 嘗試匯入 V3
try:
    from nba_api.stats.endpoints import boxscoresummaryv2, boxscoresummaryv3
    HAS_V3 = True
except ImportError:
    from nba_api.stats.endpoints import boxscoresummaryv2
    HAS_V3 = False
    print("⚠️ 警告: 你的 nba_api 版本較舊，可能無法支援 V3。")

# ==========================================
# 🔥 新增：強制注入 curl_cffi 突破 Cloudflare 防線
# ==========================================
from curl_cffi import requests as cffi_requests
from nba_api.stats.library.http import NBAStatsHTTP

def custom_send_api_request(url, parameters, headers, timeout=None, proxies=None):
    """攔截 nba_api 的底層請求，改用 curl_cffi 進行 TLS 指紋偽裝"""
    
    # 處理 Proxy 格式
    cffi_proxies = None
    if proxies:
        cffi_proxies = proxies
    elif os.environ.get('HTTP_PROXY'):
        cffi_proxies = {
            "http": os.environ.get('HTTP_PROXY'),
            "https": os.environ.get('HTTPS_PROXY')
        }

    # 使用 curl_cffi 發送偽裝請求
    resp = cffi_requests.get(
        url, 
        params=parameters, 
        headers=headers, 
        proxies=cffi_proxies,
        timeout=timeout or 30,
        impersonate="chrome120"  # 👈 偽裝成較新的 Chrome 版本
    )
    
    # 建立一個假裝是原生 requests.Response 的物件，讓 nba_api 能正常解析
    class DummyResponse:
        def __init__(self, r):
            self.status_code = r.status_code
            self.url = r.url
            self.text = r.text
            self._json = None
            try:
                self._json = r.json()
            except:
                pass
        def json(self): 
            if self._json is None:
                raise ValueError("JSON decode error")
            return self._json
            
    return DummyResponse(resp)

# 執行覆寫：讓 nba_api 全面改走我們的偽裝通道
NBAStatsHTTP.send_api_request = custom_send_api_request
# ==========================================

# 強制忽略所有警告
warnings.filterwarnings("ignore")

# ===========================
# ⚙️ 雲端自動化設定區
# ===========================
DB_PATH = 'data/nba_current.db'  # 👈 改為讀寫輕量級的新資料庫

# 定義新賽季的 GAME_ID 前綴 (與 CSV 切割一致)
NEW_SEASON_PREFIXES = ('0025', '00225', '00425')

# === 防鎖定設定 ===
MAX_WORKERS = 2         # 雲端 Proxy 建議稍微調降並行數，避免連線被 Webshare 視為惡意攻擊
BASE_DELAY = (0.6, 1.5) 
ERROR_DELAY = 10      

# ===========================
# 🛡️ Proxy 代理伺服器設定
# ===========================
def setup_proxy():
    """從 GitHub Secrets 讀取專屬 Proxy 並設定為全域環境變數"""
    proxy_url = os.environ.get('PROXY_URL')
    if proxy_url:
        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url
        print("✅ 已成功載入 Webshare 私人 Proxy 設定！")
    else:
        print("⚠️ 警告：未偵測到 PROXY_URL 環境變數，將使用 GitHub 預設 IP 連線（極可能被擋）。")

# === 🔥 終極防護：真實瀏覽器偽裝 ===
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
]

def get_headers():
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

def init_db():
    if not os.path.exists('data'): os.makedirs('data')
    # 加入 timeout=15 避免雲端硬碟 I/O 延遲造成的 Database is locked 報錯
    conn = sqlite3.connect(DB_PATH, timeout=15)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS inactive_players (
            GAME_ID TEXT,
            TEAM_ID INTEGER,
            PLAYER_ID INTEGER,
            PLAYER_NAME TEXT,
            JERSEY_NUM TEXT,
            PRIMARY KEY (GAME_ID, PLAYER_ID)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS empty_inactive_games (
            game_id TEXT PRIMARY KEY
        )
    ''')
    conn.commit()
    return conn

def clean_false_empty_games(conn):
    """自動修復：刪除那些被錯誤標記為 '空' 的 2025-26 賽季比賽"""
    try:
        cursor = conn.cursor()
        # 使用嚴謹的前綴比對，取代 LIKE '%225%'
        query = "SELECT game_id FROM empty_inactive_games"
        cursor.execute(query)
        all_empty_ids = [row[0] for row in cursor.fetchall()]
        
        suspicious_ids = [gid for gid in all_empty_ids if str(gid).startswith(NEW_SEASON_PREFIXES)]
        
        if suspicious_ids:
            print(f"🧹 發現 {len(suspicious_ids)} 場疑似誤判為空的比賽，正在清除重抓...")
            placeholders = ','.join(['?'] * len(suspicious_ids))
            cursor.execute(f"DELETE FROM empty_inactive_games WHERE game_id IN ({placeholders})", suspicious_ids)
            conn.commit()
    except Exception as e:
        pass

def get_missing_game_ids(conn):
    try:
        clean_false_empty_games(conn)

        base_df = pd.read_sql("SELECT DISTINCT GAME_ID FROM boxscore_base", conn)
        base_df.columns = [c.upper() for c in base_df.columns]
        all_games = set(base_df['GAME_ID'].astype(str).tolist())
        
        inactive_df = pd.read_sql("SELECT DISTINCT game_id FROM inactive_players", conn)
        inactive_df.columns = [c.upper() for c in inactive_df.columns]
        existing_games = set(inactive_df['GAME_ID'].astype(str).tolist()) if not inactive_df.empty else set()
        
        try:
            empty_df = pd.read_sql("SELECT DISTINCT game_id FROM empty_inactive_games", conn)
            empty_df.columns = [c.upper() for c in empty_df.columns]
            empty_games = set(empty_df['GAME_ID'].astype(str).tolist())
        except:
            empty_games = set()
            
        missing_games = sorted(list(all_games - existing_games - empty_games))
        return missing_games
    except Exception as e:
        print(f"❌ 讀取 Game ID 失敗: {e}")
        return []

def fetch_from_v3(game_id):
    """使用 V3 API 解析資料"""
    if not HAS_V3: return pd.DataFrame()
    
    try:
        data = boxscoresummaryv3.BoxScoreSummaryV3(
            game_id=game_id, 
            headers=get_headers(), 
            timeout=20
        ).get_dict()
        
        summary = data.get('boxScoreSummary', {})
        all_inactives = []
        
        for team_key in ['homeTeam', 'awayTeam']:
            team_data = summary.get(team_key, {})
            team_id = team_data.get('teamId')
            inactives_list = team_data.get('inactives', [])
            
            for p in inactives_list:
                entry = {
                    'GAME_ID': game_id,
                    'TEAM_ID': team_id,
                    'PLAYER_ID': p.get('personId'),  
                    'FIRST_NAME': p.get('firstName'), 
                    'LAST_NAME': p.get('familyName'), 
                    'JERSEY_NUM': p.get('jerseyNum')  
                }
                if entry['FIRST_NAME'] and entry['LAST_NAME']:
                    entry['PLAYER_NAME'] = f"{entry['FIRST_NAME']} {entry['LAST_NAME']}"
                else:
                    entry['PLAYER_NAME'] = 'Unknown'
                    
                all_inactives.append(entry)
            
        if not all_inactives:
            return pd.DataFrame() 
            
        return pd.DataFrame(all_inactives)
        
    except Exception as e:
        raise e

def fetch_worker(game_id):
    time.sleep(random.uniform(*BASE_DELAY))
    game_id_str = str(game_id)
    
    # 精準判斷是否為 2025-26 以後的賽季
    is_new_season = game_id_str.startswith(NEW_SEASON_PREFIXES)
    
    df = pd.DataFrame()
    
    try:
        if is_new_season and HAS_V3:
            df = fetch_from_v3(game_id_str)
        else:
            boxscore = boxscoresummaryv2.BoxScoreSummaryV2(
                game_id=game_id_str, 
                headers=get_headers(), 
                timeout=20
            )
            df = boxscore.inactive_players.get_data_frame()
            if df.empty and HAS_V3:
                df = fetch_from_v3(game_id_str)

        if df.empty:
            return ('empty', game_id_str, None)
        
        # 資料清洗與標準化
        df['GAME_ID'] = game_id_str
        
        if 'PLAYER_NAME' not in df.columns:
            if 'FIRST_NAME' in df.columns:
                df['PLAYER_NAME'] = df['FIRST_NAME'] + " " + df['LAST_NAME']
            else:
                df['PLAYER_NAME'] = 'Unknown'
                
        df.columns = [c.upper() for c in df.columns]
        needed_cols = ['GAME_ID', 'TEAM_ID', 'PLAYER_ID', 'PLAYER_NAME', 'JERSEY_NUM']
        
        for col in needed_cols:
            if col not in df.columns:
                df[col] = None 
                
        clean_df = df[needed_cols].copy()
        return ('success', game_id_str, clean_df)

    except (ReadTimeout, ConnectTimeout, ConnectionError, ProtocolError):
        time.sleep(ERROR_DELAY)
        return ('error', game_id_str, "連線超時")
    except Exception as e:
        time.sleep(ERROR_DELAY)
        return ('error', game_id_str, str(e))

def fetch_inactive_players(conn):
    missing_ids = get_missing_game_ids(conn)
    total_tasks = len(missing_ids)
    print(f"🚀 尚有 {total_tasks} 場比賽需要更新傷兵名單...")
    
    if total_tasks == 0:
        print("✅ 所有傷兵名單已是最新的。")
        return

    success_cnt = 0
    empty_cnt = 0
    error_cnt = 0
    
    print(f"開始多執行緒抓取 (Workers: {MAX_WORKERS})...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_game = {executor.submit(fetch_worker, gid): gid for gid in missing_ids}
        
        completed = 0
        for future in concurrent.futures.as_completed(future_to_game):
            completed += 1
            status, game_id, result = future.result()
            
            if status == 'success':
                try:
                    result.to_sql('inactive_players', conn, if_exists='append', index=False)
                    success_cnt += 1
                except: pass
            elif status == 'empty':
                conn.execute("INSERT OR IGNORE INTO empty_inactive_games (game_id) VALUES (?)", (game_id,))
                conn.commit()
                empty_cnt += 1
            else:
                error_cnt += 1
            
            if completed % 5 == 0 or completed == total_tasks:
                print(f"\r進度: {completed}/{total_tasks} | 成功: {success_cnt} | 空: {empty_cnt} | 失敗: {error_cnt}", end="")

    print(f"\n傷兵名單更新完成！(成功: {success_cnt}, 無傷兵: {empty_cnt}, 失敗: {error_cnt})")

if __name__ == "__main__":
    print("🚀 啟動 NBA 傷兵名單爬蟲 (雲端全自動更新版)")
    setup_proxy()
    
    conn = init_db()
    try:
        fetch_inactive_players(conn)
    finally:
        conn.close()