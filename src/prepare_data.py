import os
import sqlite3
import urllib.request
import urllib.error
import time
import pandas as pd

# ===========================
# ⚙️ 設定區
# ===========================
# 🎯 更新為你最新上傳的歷史資料庫網址
HISTORICAL_DB_URL = "https://github.com/Ricky0627/nba-api/releases/download/v1.0-data/nba_historical.db"

# 為了避免跟原本的檔名搞混，我們在雲端下載時幫它換個名字
HISTORICAL_DB_PATH = "data/nba_raw_historical.db"

# 這是 GitHub Actions 每天會抓取的最新賽季小資料庫
CURRENT_DB_PATH = "data/nba_current.db"

def download_historical_db():
    """自動從 GitHub Releases 下載歷史資料庫 (附帶防斷線重試機制)"""
    if not os.path.exists("data"):
        os.makedirs("data")
        
    if not os.path.exists(HISTORICAL_DB_PATH):
        print(f"⬇️ 正在從 GitHub Releases 下載歷史資料庫...")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # GitHub Actions 的網速極快，通常幾十秒內就能載完
                urllib.request.urlretrieve(HISTORICAL_DB_URL, HISTORICAL_DB_PATH)
                print("✅ 歷史資料庫下載完成！")
                break
            except (urllib.error.URLError, ConnectionResetError) as e:
                print(f"   ⚠️ 下載中斷 ({e})，正在進行第 {attempt + 1}/{max_retries} 次重試...")
                time.sleep(5)
                if attempt == max_retries - 1:
                    print("❌ 歷史資料庫下載失敗，請檢查網址或網路狀態。")
                    raise e
    else:
        print("✅ 歷史資料庫已存在本機，跳過下載。")

def get_merged_dataframe(table_name):
    """
    獲取合併後的完整資料表 (Pandas DataFrame 格式)
    這可以直接餵給你的機器學習模型！
    """
    download_historical_db()
    
    print(f"\n🔄 正在合併資料表: {table_name}")
    
    # --- 1. 讀取歷史資料 (冷資料) ---
    conn_hist = sqlite3.connect(HISTORICAL_DB_PATH, timeout=15.0)
    
    # 智慧判斷欄位名稱
    cursor = conn_hist.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [info[1].upper() for info in cursor.fetchall()]
    
    if 'SEASON_YEAR' in columns:
        query_hist = f"SELECT * FROM {table_name} WHERE SEASON_YEAR != '2025-26'"
    elif 'SEASON' in columns:
        query_hist = f"SELECT * FROM {table_name} WHERE season != '2025-26'"
    else:
        query_hist = f"SELECT * FROM {table_name}"
        
    df_hist = pd.read_sql(query_hist, conn_hist)
    conn_hist.close()
    
    # --- 2. 讀取最新資料 (熱資料) ---
    if os.path.exists(CURRENT_DB_PATH):
        conn_curr = sqlite3.connect(CURRENT_DB_PATH, timeout=15.0)
        df_curr = pd.read_sql(f"SELECT * FROM {table_name}", conn_curr)
        conn_curr.close()
    else:
        print(f"⚠️ 找不到最新資料庫 {CURRENT_DB_PATH}，僅使用歷史資料。")
        df_curr = pd.DataFrame()
        
    # --- 3. 兩者合體 ---
    df_merged = pd.concat([df_hist, df_curr], ignore_index=True)
    
    # 針對沒有賽季欄位的關聯表，進行去重保護
    if 'SEASON_YEAR' not in columns and 'SEASON' not in columns:
        df_merged = df_merged.drop_duplicates()
        
    print(f"   📊 歷史: {len(df_hist)} 筆 | 🆕 最新: {len(df_curr)} 筆 | 🚀 總計: {len(df_merged)} 筆")
    return df_merged

if __name__ == "__main__":
    print("🚀 啟動 NBA 數據合併系統")
    df_advanced = get_merged_dataframe("boxscore_advanced")
    if not df_advanced.empty:
        print("\n🏆 合併大成功！顯示前 3 筆與最後 3 筆資料確認：")
        print(pd.concat([df_advanced.head(3), df_advanced.tail(3)]).to_markdown(index=False))