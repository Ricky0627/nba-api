import sqlite3
import pandas as pd
import time
import random
import os
import sys
import threading
import concurrent.futures
from curl_cffi import requests as cffi_requests

# ==========================================
# ⚙️ 雲端自動化設定區 (PBP 逐球文字轉播)
# ==========================================
DB_PATH = 'data/nba_current.db'  # 👈 改為雲端版的輕量級資料庫
OUTPUT_CSV = 'data/current_play_by_play.csv'  # 👈 指向當前賽季的新 CSV 檔案

# 🛡️ PBP 資料量大，為了保護 Proxy IP，將並行數調降為 3
MAX_WORKERS = 3

csv_lock = threading.Lock()
PROXY_DICT = None

# ===========================
# 🛡️ Proxy 代理伺服器設定
# ===========================
def setup_proxy():
    """從 GitHub Secrets 讀取專屬 Proxy 並設定為全域環境變數"""
    global PROXY_DICT
    proxy_url = os.environ.get('PROXY_URL')
    if proxy_url:
        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url
        # curl_cffi 需要的 proxy 格式
        PROXY_DICT = {
            "http": proxy_url,
            "https": proxy_url
        }
        print("✅ 已成功載入 Webshare 私人 Proxy 設定！")
    else:
        print("⚠️ 警告：未偵測到 PROXY_URL 環境變數，將使用 GitHub 預設 IP 連線（極可能被擋）。")

def get_vip_headers():
    return {
        'Host': 'stats.nba.com',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'x-nba-stats-origin': 'stats',
        'x-nba-stats-token': 'true',
        'Referer': 'https://www.nba.com/',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }

def fetch_single_game_pbp(gid):
    # 🎯 升級武器：直接敲擊最新版的 v3 端點！
    url = "https://stats.nba.com/stats/playbyplayv3"
    
    params = {
        "GameID": str(gid).zfill(10),
        "StartPeriod": 0,
        "EndPeriod": 0
    }
    
    headers = get_vip_headers()

    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 加入 proxies=PROXY_DICT 透過代理伺服器請求
            response = cffi_requests.get(
                url, 
                params=params, 
                headers=headers,
                proxies=PROXY_DICT, 
                timeout=20,
                impersonate="chrome110" 
            )
            
            if response.status_code != 200:
                print(f"   ⚠️ [HTTP {response.status_code}] 請求遭拒。")
                time.sleep(random.uniform(3, 6))
                continue
                
            data = response.json()
            
            # 💡 針對 v3 新版 JSON 的解析法
            if 'game' in data and 'actions' in data['game']:
                actions = data['game']['actions']
                
                if not actions:
                    return None
                    
                # 直接把 actions 列表轉換成 Pandas DataFrame
                df_pbp = pd.DataFrame(actions)
                
                # 確保我們有比賽 ID 可以追蹤
                df_pbp['GAME_ID'] = str(gid).zfill(10)
                
                return df_pbp
            else:
                print(f"\n   🚨 [API 未知格式] 回傳內容: {str(data)[:200]}...\n")
                return None
            
        except Exception as e:
            print(f"   ⚡ [嘗試 {attempt+1}] 連線異常: {str(e)[:50]}")
            time.sleep(random.uniform(3, 6))
            
    return None

def worker_task(gid):
    # 🚥 起跑前微微錯開
    time.sleep(random.uniform(0.1, 1.5)) 
    
    df_game = fetch_single_game_pbp(gid)
    
    if df_game is not None and not df_game.empty:
        with csv_lock:
            # 確保 data 資料夾存在
            os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
            file_exists = os.path.exists(OUTPUT_CSV)
            
            # 一次寫入一場比賽的所有事件
            df_game.to_csv(OUTPUT_CSV, mode='a', index=False, header=not file_exists, encoding='utf-8-sig')
            
            event_count = len(df_game)
            print(f"   ✅ [PBP 存檔] 比賽 {gid} 完成！(共 {event_count} 筆事件)")
            
        # 💤 成功後，強制發呆一段時間，保護 Proxy 不被短暫封鎖
        time.sleep(random.uniform(4.0, 7.0))
    else:
        print(f"   ❌ [徹底失敗] 比賽 {gid} 放棄。")
        time.sleep(random.uniform(8.0, 12.0))

def run_pbp_fetch():
    print(f"🚀 [MLOps] 啟動 PBP 增量更新爬蟲 (開啟 {MAX_WORKERS} 個分身)...")

    if not os.path.exists(DB_PATH):
        print(f"❌ 找不到資料庫 {DB_PATH}，請先執行前置的數據抓取腳本！")
        return

    # 1️⃣ 從 SQLite 撈取「應該要有」的所有比賽名單
    try:
        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        # 雲端資料庫只包含當前賽季，全撈即可
        query = "SELECT DISTINCT game_id FROM games"
        df_games = pd.read_sql(query, conn)
        conn.close()
        all_gids = df_games['game_id'].astype(str).str.zfill(10).tolist()
    except Exception as e:
        print(f"❌ 無法讀取資料庫: {e}")
        return

    # 2️⃣ 讀取現有 CSV，建立「已經抓過」的防護網 (增量更新核心)
    completed_gids = set()
    if os.path.exists(OUTPUT_CSV):
        try:
            print("🔍 正在讀取既有 PBP 檔案，比對已下載的比賽...")
            # 只讀取 GAME_ID 欄位，並使用 unique() 壓縮記憶體
            df_existing = pd.read_csv(OUTPUT_CSV, usecols=['GAME_ID'], dtype={'GAME_ID': str})
            completed_gids = set(df_existing['GAME_ID'].dropna().str.zfill(10).unique())
            print(f"📂 發現 {len(completed_gids)} 場已存在資料，將自動跳過！")
        except Exception as e:
            print(f"⚠️ 讀取既有 CSV 失敗: {e}。將重新開始檢查。")

    # 3️⃣ 兩者相減，得出「真正需要抓」的清單
    missing_gids = [g for g in all_gids if g not in completed_gids]
    
    if not missing_gids:
        print("🎉 資料庫已是最新狀態，沒有新的比賽需要抓取囉！")
        return
        
    print(f"🎯 鎖定 {len(missing_gids)} 場未紀錄比賽，準備出擊...")

    # 4️⃣ 啟動多執行緒抓取
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures = []
    
    try:
        for gid in missing_gids:
            futures.append(executor.submit(worker_task, gid))
            
        for future in concurrent.futures.as_completed(futures):
            future.result() 
            
    except KeyboardInterrupt:
        print("\n\n🛑 偵測到手動中斷 (Ctrl+C)！進度已安全儲存。")
        executor.shutdown(wait=False, cancel_futures=True)
        sys.exit(0)

if __name__ == "__main__":
    setup_proxy()
    run_pbp_fetch()