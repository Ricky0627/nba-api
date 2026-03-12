import pandas as pd
import sqlite3
import time
import random
import os
import datetime
import warnings
import pytz  # 👈 新增：處理時區問題
from nba_api.stats.endpoints import playergamelogs
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError
from urllib3.exceptions import ProtocolError
from tqdm import tqdm

# ==========================================
# 🔥 終極防護罩：底層 requests 核心替換 (防彈版)
# ==========================================
import nba_api.stats.library.http as nba_http
import os

class CFFIMockRequests:
    @staticmethod
    def get(url, **kwargs):
        return CFFIMockRequests._cffi_get(url, **kwargs)
        
    class Session:
        def get(self, url, **kwargs):
            return CFFIMockRequests._cffi_get(url, **kwargs)
            
    @staticmethod
    def _cffi_get(url, **kwargs):
        kwargs['impersonate'] = "chrome120"
        
        if 'proxies' not in kwargs and os.environ.get('HTTP_PROXY'):
            kwargs['proxies'] = {
                "http": os.environ.get('HTTP_PROXY'),
                "https": os.environ.get('HTTPS_PROXY')
            }
        
        from curl_cffi import requests as cffi_requests
        from requests.exceptions import ReadTimeout, ConnectionError
        
        try:
            resp = cffi_requests.get(url, **kwargs)
        except Exception as e:
            if "timeout" in str(e).lower(): raise ReadTimeout(e)
            raise ConnectionError(e)
            
        class DummyResponse:
            def __init__(self, r):
                self.status_code = r.status_code
                self.url = r.url
                self.text = r.text
                self.content = r.content
                self.headers = getattr(r, 'headers', {})
                self._json = None
                try: self._json = r.json()
                except: pass
            def json(self):
                if self._json is None: raise ValueError("JSON decode error")
                return self._json
                
        return DummyResponse(resp)

nba_http.requests = CFFIMockRequests
# ==========================================

# 忽略警告
warnings.filterwarnings("ignore", category=UserWarning, module="nba_api")

# ===========================
# ⚙️ 雲端自動化設定區
# ===========================
DB_PATH = 'data/nba_current.db'  # 👈 改為讀寫輕量級的新資料庫
START_YEAR = 2025                # 👈 雲端只負責當前賽季
END_YEAR = 2026
SEASON_TYPES = ['Regular Season', 'Playoffs']
MEASURE_TYPES = ['Base', 'Advanced', 'Misc', 'Scoring', 'Usage']

TIMEOUT_SECONDS = 30             # 使用私人 Proxy，速度快，超時可以縮短
MAX_RETRIES = 5
RETRY_DELAY = 3

# 👈 新增：設定 NBA 官方時間基準 (美國東岸時間)
EST_TZ = pytz.timezone('US/Eastern')

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
    """每次請求隨機產生一組正常的瀏覽器標頭"""
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
    # 👈 新增：加入 timeout=15.0 防止並行鎖定報錯
    return sqlite3.connect(DB_PATH, timeout=15.0)

def is_current_season(season_str):
    start_year = int(season_str.split('-')[0])
    # 👈 修改：使用美東時間
    current_year = datetime.datetime.now(EST_TZ).year
    return start_year >= (current_year - 1)

def is_future_playoffs(season_str):
    start_year = int(season_str.split('-')[0])
    playoff_year = start_year + 1 
    # 👈 修改：使用美東時間
    now = datetime.datetime.now(EST_TZ)
    if now.year < playoff_year or (now.year == playoff_year and now.month < 4):
        return True
    return False

def check_season_status(conn, table_name, season, season_type):
    if is_current_season(season):
        return 'UPDATE'
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone(): return 'EMPTY'
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE SEASON_YEAR = ? AND SEASON_TYPE = ?", (season, season_type))
        if cursor.fetchone()[0] > 1000: return 'SKIP'
        return 'EMPTY'
    except:
        return 'EMPTY'

def get_latest_date(conn, table_name, season, season_type):
    try:
        cursor = conn.cursor()
        query = f"SELECT MAX(GAME_DATE) FROM {table_name} WHERE SEASON_YEAR = ? AND SEASON_TYPE = ?"
        cursor.execute(query, (season, season_type))
        res = cursor.fetchone()
        if res and res[0]:
            y, m, d = res[0][:10].split('-')
            return f"{m}/{d}/{y}"
    except:
        pass
    return ""

def save_to_db_incremental(conn, df, table_name):
    if df.empty: return
    try:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        
        if columns_info: 
            existing_cols = [info[1] for info in columns_info]
            for col in df.columns:
                if col not in existing_cols:
                    # 👈 新增：加上容錯處理
                    try:
                        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT")
                    except:
                        pass
            conn.commit()

        df.head(0).to_sql(table_name, conn, if_exists='append', index=False)
        try:
            existing = pd.read_sql(f"SELECT GAME_ID, PLAYER_ID FROM {table_name}", conn)
            existing_keys = set(existing['GAME_ID'].astype(str) + '_' + existing['PLAYER_ID'].astype(str))
        except:
            existing_keys = set()
        
        df_keys = df['GAME_ID'].astype(str) + '_' + df['PLAYER_ID'].astype(str)
        new_data = df[~df_keys.isin(existing_keys)].copy()
        
        if not new_data.empty:
            new_data.to_sql(table_name, conn, if_exists='append', index=False)
    except Exception as e:
        tqdm.write(f"   ❌ 寫入資料庫錯誤: {e}")

def fetch_with_retry(season, season_type, measure_type, date_from=None, date_to=None):
    for attempt in range(MAX_RETRIES):
        try:
            # 🔥 將偽裝的 Headers 帶入請求中
            logs = playergamelogs.PlayerGameLogs(
                season_nullable=season,
                season_type_nullable=season_type,
                measure_type_player_game_logs_nullable=measure_type,
                date_from_nullable=date_from,
                date_to_nullable=date_to,
                headers=get_headers(),  
                timeout=TIMEOUT_SECONDS
            )
            return logs.get_data_frames()[0]
            
        except (ReadTimeout, ConnectTimeout, ConnectionError, ProtocolError) as e:
            wait_time = RETRY_DELAY + (attempt * 3) + random.uniform(2, 4)
            error_brief = str(e).split("',")[-1].strip(" )\"'")[:30]
            tqdm.write(f"   ⚠️ 伺服器無回應，等 {wait_time:.1f} 秒重試... [{error_brief}]")
            time.sleep(wait_time)
            
        except Exception as e:
            if "no data" in str(e).lower() or "timeout" in str(e).lower():
                break
            tqdm.write(f"   ⚠️ API 未知錯誤: {e}")
            break
            
    return pd.DataFrame()

def fetch_player_stats(conn):
    seasons = [f"{y}-{str(y+1)[-2:]}" for y in range(START_YEAR, END_YEAR)]
    total_steps = len(seasons) * len(SEASON_TYPES) * len(MEASURE_TYPES)
    pbar = tqdm(total=total_steps, desc="下載球員全賽季數據")

    for season in seasons:
        for s_type in SEASON_TYPES:
            if s_type == 'Playoffs' and is_future_playoffs(season):
                tqdm.write(f"   ⏭️ [跳過] {season} 季後賽尚未開打。")
                pbar.update(len(MEASURE_TYPES)) 
                continue

            for m_type in MEASURE_TYPES:
                safe_name = m_type.lower().replace(' ', '_')
                table_name = f"player_stats_{safe_name}"
                
                status = check_season_status(conn, table_name, season, s_type)
                if status == 'SKIP':
                    pbar.update(1)
                    continue
                
                # ==========================================
                # 🔥 逐日切割法更新雲端最新進度
                # ==========================================
                if status == 'UPDATE' and is_current_season(season):
                    last_date_str = get_latest_date(conn, table_name, season, s_type)
                    
                    if not last_date_str:
                        last_date_str = f"10/15/{season.split('-')[0]}"
                    
                    start_dt = datetime.datetime.strptime(last_date_str, "%m/%d/%Y")
                    start_dt -= datetime.timedelta(days=1) 
                    
                    # 👈 修改：使用美東時間計算抓取迄日，並去掉 tzinfo 以利運算
                    end_dt = datetime.datetime.now(EST_TZ).replace(tzinfo=None)

                    date_list = []
                    curr = start_dt
                    while curr <= end_dt:
                        date_list.append(curr.strftime("%m/%d/%Y"))
                        curr += datetime.timedelta(days=1)

                    if m_type == 'Base': 
                        tqdm.write(f"   📅 [{season} {s_type}] 啟動逐日抓取模式，補齊 {len(date_list)} 天的資料...")

                    success_days = 0
                    for d in date_list:
                        df = fetch_with_retry(season, s_type, m_type, date_from=d, date_to=d)
                        if not df.empty:
                            df['SEASON_YEAR'] = season
                            df['SEASON_TYPE'] = s_type
                            df['MEASURE_TYPE'] = m_type
                            save_to_db_incremental(conn, df, table_name)
                            success_days += 1
                        
                        # 🔥 加上隨機微小延遲，模仿人類行為
                        time.sleep(random.uniform(0.6, 1.5))
                        
                    tqdm.write(f"   ✅ [{table_name}] 成功補齊 {success_days} 天有比賽的數據。")
                        
                else:
                    tqdm.write(f"   ⏳ [{table_name}] 請求 {season} 完整資料...")
                    df = fetch_with_retry(season, s_type, m_type)
                    if not df.empty:
                        df['SEASON_YEAR'] = season
                        df['SEASON_TYPE'] = s_type
                        df['MEASURE_TYPE'] = m_type
                        save_to_db_incremental(conn, df, table_name)
                    time.sleep(random.uniform(1.5, 3.0))

                pbar.update(1)

    pbar.close()
    print("球員數據下載完成！")

if __name__ == "__main__":
    print(f"🚀 啟動 NBA 球員數據爬蟲 (雲端全自動更新版)")
    # 初始化 Proxy
    setup_proxy()
    
    conn = init_db()
    try:
        fetch_player_stats(conn)
    finally:
        conn.close()