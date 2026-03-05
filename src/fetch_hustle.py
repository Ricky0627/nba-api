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
# ⚙️ 雲端自動化設定區 (TLS 完美偽裝版)
# ==========================================
DB_PATH = 'data/nba_current.db'  # 👈 改為雲端版的輕量級資料庫
TABLE_NAME = "boxscore_hustle"

# 雲端環境建議將並行數調降，避免被 Proxy 供應商或 NBA 防火牆視為 DDoS 攻擊
MAX_WORKERS = 3  

# 改用 db_lock 來防止多執行緒同時寫入 SQLite 造成的衝突
db_lock = threading.Lock()

# 全域 Proxy 字典，供 curl_cffi 使用
PROXY_DICT = None

# ===========================
# 🛡️ Proxy 代理伺服器設定
# ===========================
def setup_proxy():
    """從 GitHub Secrets 讀取專屬 Proxy，避免帳密外洩在程式碼中"""
    global PROXY_DICT
    proxy_url = os.environ.get('PROXY_URL')
    if proxy_url:
        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url
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

def fetch_single_game_hustle_cffi(gid):
    url = "https://stats.nba.com/stats/boxscorehustlev2"
    
    params = {
        "GameID": str(gid).zfill(10),
        "StartPeriod": 0,
        "EndPeriod": 0,
        "StartRange": 0,
        "EndRange": 0,
        "RangeType": 0
    }
    
    headers = get_vip_headers()

    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 使用 curl_cffi 完美偽裝成 Chrome 110，避開 Cloudflare 阻擋
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
                time.sleep(random.uniform(1.5, 3.5))
                continue
                
            data = response.json()
            
            # 💡 檢查是否為新版 JSON 格式
            if 'boxScoreHustle' in data:
                hustle_data = data['boxScoreHustle']
                
                home_stats = hustle_data['homeTeam']['statistics']
                home_stats['TEAM_ID'] = hustle_data['homeTeam']['teamId']
                home_stats['TEAM_ABBREVIATION'] = hustle_data['homeTeam']['teamTricode']
                
                away_stats = hustle_data['awayTeam']['statistics']
                away_stats['TEAM_ID'] = hustle_data['awayTeam']['teamId']
                away_stats['TEAM_ABBREVIATION'] = hustle_data['awayTeam']['teamTricode']
                
                combined_stats = [home_stats, away_stats]
                df_team = pd.DataFrame(combined_stats)
                df_team['GAME_ID'] = str(gid).zfill(10)
                
                target_cols_mapping = {
                    'GAME_ID': 'GAME_ID', 'TEAM_ID': 'TEAM_ID', 'TEAM_ABBREVIATION': 'TEAM_ABBREVIATION', 
                    'minutes': 'MIN', 'contestedShots': 'CONTESTED_SHOTS', 'contestedShots3pt': 'CONTESTED_SHOTS_3PT',
                    'deflections': 'DEFLECTIONS', 'chargesDrawn': 'CHARGES_DRAWN', 'screenAssists': 'SCREEN_ASSISTS',
                    'screenAssistPoints': 'SCREEN_AST_PTS', 'looseBallsRecoveredTotal': 'LOOSE_BALLS_RECOVERED', 'boxOuts': 'BOX_OUTS'
                }
                
                available_cols = [c for c in target_cols_mapping.keys() if c in df_team.columns]
                df_selected = df_team[available_cols].copy()
                df_selected.rename(columns=target_cols_mapping, inplace=True)
                return df_selected
                
            # 舊版解析法備用
            elif 'resultSets' in data:
                team_set = next((rs for rs in data['resultSets'] if rs['name'] == 'HustleStatsTeam'), None)
                if not team_set: return None
                df_team = pd.DataFrame(team_set['rowSet'], columns=team_set['headers'])
                df_team['GAME_ID'] = str(gid).zfill(10)
                
                target_cols_mapping = {
                    'GAME_ID': 'GAME_ID', 'TEAM_ID': 'TEAM_ID', 'TEAM_ABBREVIATION': 'TEAM_ABBREVIATION', 
                    'MINUTES': 'MIN', 'CONTESTED_SHOTS': 'CONTESTED_SHOTS', 'CONTESTED_SHOTS_3PT': 'CONTESTED_SHOTS_3PT',
                    'DEFLECTIONS': 'DEFLECTIONS', 'CHARGES_DRAWN': 'CHARGES_DRAWN', 'SCREEN_ASSISTS': 'SCREEN_ASSISTS',
                    'SCREEN_ASSIST_POINTS': 'SCREEN_AST_PTS', 'LOOSE_BALLS_RECOVERED': 'LOOSE_BALLS_RECOVERED', 'BOX_OUTS': 'BOX_OUTS'
                }
                available_cols = [c for c in target_cols_mapping.keys() if c in df_team.columns]
                df_selected = df_team[available_cols].copy()
                df_selected.rename(columns=target_cols_mapping, inplace=True)
                return df_selected
                
            else:
                return None
            
        except Exception as e:
            print(f"   ⚡ [嘗試 {attempt+1}] 解析或連線異常: {str(e)[:50]}")
            time.sleep(random.uniform(2, 4))
            
    return None

def worker_task(gid):
    time.sleep(random.uniform(0.5, 2.0)) 
    
    df_game = fetch_single_game_hustle_cffi(gid)
    
    if df_game is not None and not df_game.empty:
        # 🔒 使用 lock 確保同一時間只有一個 Thread 能寫入 SQLite
        with db_lock:
            try:
                # timeout=20 確保如果資料庫忙碌，會等待一下而不是直接報錯
                conn = sqlite3.connect(DB_PATH, timeout=20.0)
                # 使用 append 模式增量寫入
                df_game.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
                conn.close()
                print(f"   ✅ [DB 寫入] 比賽 {gid} Hustle 存檔完成！")
            except Exception as e:
                print(f"   ❌ [DB 錯誤] 比賽 {gid} 寫入資料庫失敗: {e}")
                
        time.sleep(random.uniform(1.0, 2.0))
    else:
        print(f"   ❌ [徹底失敗] 比賽 {gid} 放棄。")

def run_hustle_fetch():
    print(f"🚀 [MLOps] 啟動 Hustle 直接入庫增量爬蟲 (開啟 {MAX_WORKERS} 個分身)...")

    if not os.path.exists(DB_PATH):
        print(f"❌ 找不到資料庫 {DB_PATH}，請先確認是否已同步基礎數據！")
        return

    # 1️⃣ 從 SQLite 撈取「應該要有」的所有比賽名單
    try:
        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        # 因為是雲端小資料庫，裡面只會有最新賽季的比賽，所以直接全撈即可
        query = "SELECT DISTINCT game_id FROM games"
        df_games = pd.read_sql(query, conn)
        all_gids = df_games['game_id'].astype(str).str.zfill(10).tolist()
    except Exception as e:
        print(f"❌ 無法讀取資料庫的 games 表: {e}")
        return

    # 2️⃣ 直接從資料庫的 boxscore_hustle 讀取「已經抓過」的比賽清單
    completed_gids = set()
    try:
        # 直接去查我們剛剛建立的表
        query_existing = f"SELECT DISTINCT GAME_ID FROM {TABLE_NAME}"
        df_existing = pd.read_sql(query_existing, conn)
        completed_gids = set(df_existing['GAME_ID'].astype(str).str.zfill(10).tolist())
        print(f"📂 資料庫中已發現 {len(completed_gids)} 場 Hustle 資料，將自動跳過！")
    except Exception as e:
        print(f"⚠️ 讀取 {TABLE_NAME} 失敗 (可能是全新啟動或表不存在): {e}")
    finally:
        conn.close() # 記得關閉查詢用的連線

    # 3️⃣ 兩者相減得出尚未抓取的清單
    missing_gids = [g for g in all_gids if g not in completed_gids]
    
    if not missing_gids:
        print("🎉 資料庫已是最新狀態，全部 Hustle 數據都已補齊囉！")
        return
        
    print(f"🎯 鎖定 {len(missing_gids)} 場未紀錄比賽，準備出擊...")

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures = []
    
    try:
        for gid in missing_gids:
            futures.append(executor.submit(worker_task, gid))
            
        for future in concurrent.futures.as_completed(futures):
            future.result() 
            
    except KeyboardInterrupt:
        print("\n\n🛑 偵測到手動中斷 (Ctrl+C)！進度已安全保存在資料庫。")
        executor.shutdown(wait=False, cancel_futures=True)
        sys.exit(0)

if __name__ == "__main__":
    setup_proxy()
    run_hustle_fetch()