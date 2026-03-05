import sqlite3
import os

# --- 設定區塊 ---
# 定義賽季變數
CURRENT_SEASON = "2025-26"

# 定義檔案路徑 (使用 raw string r"..." 避免 Windows 路徑反斜線轉義問題)
SOURCE_DB_PATH = r"C:\Users\ricky\OneDrive\桌面\nba_new_project\data\nba_raw.db"
HISTORICAL_DB_PATH = r"C:\Users\ricky\OneDrive\桌面\nba_new_project\data\nba_historical.db"
CURRENT_DB_PATH = r"C:\Users\ricky\OneDrive\桌面\nba-api\data\nba_current.db"

def setup_directories():
    """確保目標資料夾存在"""
    os.makedirs(os.path.dirname(HISTORICAL_DB_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(CURRENT_DB_PATH), exist_ok=True)

def split_database():
    setup_directories()

    # 若目標檔案已存在，為避免重複插入，先刪除或請確保是空的
    if os.path.exists(HISTORICAL_DB_PATH):
        os.remove(HISTORICAL_DB_PATH)
    if os.path.exists(CURRENT_DB_PATH):
        os.remove(CURRENT_DB_PATH)

    print("連接來源資料庫...")
    conn = sqlite3.connect(SOURCE_DB_PATH)
    cursor = conn.cursor()

    # 取得所有資料表名稱
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    print("初始化新的資料庫結構...")
    conn_hist = sqlite3.connect(HISTORICAL_DB_PATH)
    conn_curr = sqlite3.connect(CURRENT_DB_PATH)

    # 複製結構 (Schema) 到兩個新資料庫
    for table in tables:
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
        schema = cursor.fetchone()[0]
        if schema: # 避免某些自動生成的表沒取到
            conn_hist.execute(schema)
            conn_curr.execute(schema)

    conn_hist.commit()
    conn_curr.commit()
    conn_hist.close()
    conn_curr.close()

    print("掛載資料庫進行高速轉移...")
    # 使用 ATTACH DATABASE 進行高速轉移
    cursor.execute(f"ATTACH DATABASE '{HISTORICAL_DB_PATH}' AS hist")
    cursor.execute(f"ATTACH DATABASE '{CURRENT_DB_PATH}' AS curr")

    for table in tables:
        print(f"處理資料表: {table} ...")
        
        # 取得資料表的所有欄位名稱 (轉換為全大寫以利比對)
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [col[1] for col in cursor.fetchall()]
        col_names_upper = [c.upper() for c in columns]

        # 根據欄位特徵決定過濾策略
        if 'SEASON_YEAR' in col_names_upper:
            # 第一類：有 SEASON_YEAR (如 boxscore_base)
            col = columns[col_names_upper.index('SEASON_YEAR')]
            curr_sql = f"INSERT INTO curr.{table} SELECT * FROM {table} WHERE {col} = '{CURRENT_SEASON}'"
            hist_sql = f"INSERT INTO hist.{table} SELECT * FROM {table} WHERE {col} != '{CURRENT_SEASON}'"
            
        elif 'SEASON' in col_names_upper:
            # 第二類：有 season (如 games)
            col = columns[col_names_upper.index('SEASON')]
            curr_sql = f"INSERT INTO curr.{table} SELECT * FROM {table} WHERE {col} = '{CURRENT_SEASON}'"
            hist_sql = f"INSERT INTO hist.{table} SELECT * FROM {table} WHERE {col} != '{CURRENT_SEASON}'"
            
        elif 'GAME_ID' in col_names_upper:
            # 第三類：只有 GAME_ID，需關聯 games 表判定賽季 (如 team_features_clutch)
            col = columns[col_names_upper.index('GAME_ID')]
            curr_sql = f"""
                INSERT INTO curr.{table} 
                SELECT t.* FROM {table} t 
                JOIN games g ON t.{col} = g.game_id 
                WHERE g.season = '{CURRENT_SEASON}'
            """
            hist_sql = f"""
                INSERT INTO hist.{table} 
                SELECT t.* FROM {table} t 
                JOIN games g ON t.{col} = g.game_id 
                WHERE g.season != '{CURRENT_SEASON}'
            """
        else:
            # 例外情況：如果沒有上述任何欄位，預設全數備份到歷史資料庫 (避免資料遺失)
            print(f"  [警告] 表 {table} 缺乏賽季辨識欄位，將全數寫入歷史庫。")
            curr_sql = None
            hist_sql = f"INSERT INTO hist.{table} SELECT * FROM {table}"

        # 執行寫入
        if curr_sql:
            cursor.execute(curr_sql)
        if hist_sql:
            cursor.execute(hist_sql)

    conn.commit()
    conn.close()
    print("\n分割完成！")
    print(f"✅ 歷史庫已建立: {HISTORICAL_DB_PATH}")
    print(f"✅ 當前庫已建立: {CURRENT_DB_PATH}")

if __name__ == "__main__":
    split_database()