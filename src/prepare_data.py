import os
import sqlite3
import urllib.request
import pandas as pd

# ===========================
# ⚙️ 設定區
# ===========================
# 這是你剛剛辛苦上傳的歷史巨獸下載點
HISTORICAL_DB_URL = "https://github.com/Ricky0627/nba-api/releases/download/v1.0-data/nba_raw.db"
# 為了避免跟原本的檔名搞混，我們在雲端下載時幫它換個名字
HISTORICAL_DB_PATH = "data/nba_raw_historical.db"
# 這是 GitHub Actions 每天會抓取的最新賽季小資料庫
CURRENT_DB_PATH = "data/nba_current.db"

def download_historical_db():
    """自動從 GitHub Releases 下載歷史資料庫"""
    if not os.path.exists("data"):
        os.makedirs("data")
        
    if not os.path.exists(HISTORICAL_DB_PATH):
        print(f"⬇️ 正在從 GitHub Releases 下載歷史資料庫 (約 663MB)...")
        # GitHub Actions 的網速極快，通常幾十秒內就能載完
        urllib.request.urlretrieve(HISTORICAL_DB_URL, HISTORICAL_DB_PATH)
        print("✅ 歷史資料庫下載完成！")
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
    conn_hist = sqlite3.connect(HISTORICAL_DB_PATH)
    
    # 智慧判斷欄位名稱 (應對你資料庫中 SEASON_YEAR 與 season 混用的狀況)
    cursor = conn_hist.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [info[1] for info in cursor.fetchall()]
    
    if 'SEASON_YEAR' in columns:
        # 排除 2025-26，避免與新資料庫重複
        query_hist = f"SELECT * FROM {table_name} WHERE SEASON_YEAR != '2025-26'"
    elif 'season' in columns:
        query_hist = f"SELECT * FROM {table_name} WHERE season != '2025-26'"
    else:
        # 如果是沒有賽季欄位的表 (如 inactive_players)，就全抓
        query_hist = f"SELECT * FROM {table_name}"
        
    df_hist = pd.read_sql(query_hist, conn_hist)
    conn_hist.close()
    
    # --- 2. 讀取最新資料 (熱資料) ---
    if os.path.exists(CURRENT_DB_PATH):
        conn_curr = sqlite3.connect(CURRENT_DB_PATH)
        df_curr = pd.read_sql(f"SELECT * FROM {table_name}", conn_curr)
        conn_curr.close()
    else:
        print(f"⚠️ 找不到最新資料庫 {CURRENT_DB_PATH}，僅使用歷史資料。")
        df_curr = pd.DataFrame()
        
    # --- 3. 兩者合體 ---
    df_merged = pd.concat([df_hist, df_curr], ignore_index=True)
    
    # 針對沒有賽季欄位的關聯表，進行去重保護
    if 'SEASON_YEAR' not in columns and 'season' not in columns:
        df_merged = df_merged.drop_duplicates()
        
    print(f"   📊 歷史: {len(df_hist)} 筆 | 🆕 最新: {len(df_curr)} 筆 | 🚀 總計: {len(df_merged)} 筆")
    return df_merged

# ===========================
# 測試執行區
# ===========================
if __name__ == "__main__":
    print("🚀 啟動 NBA 數據合併系統")
    
    # 測試合併進階數據表
    df_advanced = get_merged_dataframe("boxscore_advanced")
    
    if not df_advanced.empty:
        print("\n🏆 合併大成功！顯示前 3 筆與最後 3 筆資料確認：")
        # 顯示頭尾資料，確認 2014 和 2025-26 都有包含進來
        print(pd.concat([df_advanced.head(3), df_advanced.tail(3)]).to_markdown(index=False))