import sqlite3
import pandas as pd
import os

# ===========================
# ⚙️ 雲端自動化設定區
# ===========================
DB_PATH = 'data/nba_current.db'  # 👈 改為讀寫輕量級的新資料庫

def init_games_table():
    print("🚀 正在同步賽程表 (Games Table - 雲端版)...")
    
    if not os.path.exists(DB_PATH):
        print(f"❌ 找不到資料庫 {DB_PATH}")
        return

    # 👈 新增：加入 timeout=15.0 防止雲端環境連續讀寫時發生鎖定報錯
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    c = conn.cursor()

    # 1. 建立 games 表格
    c.execute('''
        CREATE TABLE IF NOT EXISTS games (
            game_id TEXT PRIMARY KEY,
            date TEXT,
            season TEXT,
            game_type TEXT,
            home_team TEXT,
            away_team TEXT,
            home_score INTEGER,
            away_score INTEGER,
            tw_spread_score REAL,
            tw_total_score REAL,
            tw_moneyline_home REAL,
            tw_moneyline_away REAL,
            tw_spread_home_odds REAL,
            tw_spread_away_odds REAL,
            tw_total_over_odds REAL,
            tw_total_under_odds REAL
        )
    ''')
    conn.commit()

    # 2. 讀取 boxscore_base (來源)
    print("📦 讀取 Boxscore 數據...")
    try:
        # 雲端版資料庫只會有 2025-26 的資料，所以這裡會自然只處理當前賽季
        query = """
        SELECT GAME_ID, GAME_DATE, SEASON_YEAR, SEASON_TYPE, TEAM_ABBREVIATION, PTS, MATCHUP
        FROM boxscore_base
        """
        df = pd.read_sql(query, conn)
    except Exception as e:
        print(f"❌ 讀取錯誤: {e}")
        return

    if df.empty:
        print("⚠️ boxscore_base 為空，請確認是否已經執行過 fetch_data.py。")
        return

    # 3. 資料轉換
    # 去重
    df = df.drop_duplicates(subset=['GAME_ID', 'TEAM_ABBREVIATION'])
    
    # 確保 MATCHUP 不是空值，避免 str.contains 報錯
    df = df.dropna(subset=['MATCHUP'])
    
    # 分離主客場 (加上 regex=False 提高執行效率並避免警告)
    home_df = df[df['MATCHUP'].str.contains('vs.', na=False, regex=False)].rename(columns={'TEAM_ABBREVIATION': 'home_team', 'PTS': 'home_score'})
    away_df = df[df['MATCHUP'].str.contains('@', na=False, regex=False)].rename(columns={'TEAM_ABBREVIATION': 'away_team', 'PTS': 'away_score'})
    
    merged = pd.merge(
        home_df[['GAME_ID', 'GAME_DATE', 'SEASON_YEAR', 'SEASON_TYPE', 'home_team', 'home_score']],
        away_df[['GAME_ID', 'away_team', 'away_score']],
        on='GAME_ID',
        how='inner'
    )
    merged['GAME_ID'] = merged['GAME_ID'].astype(str).str.zfill(10)

    # 👈 新增：處理可能為 NaN 的分數 (例如比賽打一半或是資料缺失)，避免轉 int 報錯
    merged['home_score'] = merged['home_score'].fillna(0).astype(int)
    merged['away_score'] = merged['away_score'].fillna(0).astype(int)

    # 4. 增量寫入
    # 找出 games 表已經有的 ID
    existing_games = pd.read_sql("SELECT game_id FROM games", conn)
    existing_ids = set(existing_games['game_id'].astype(str).tolist())
    
    # 篩選新比賽
    new_games = merged[~merged['GAME_ID'].isin(existing_ids)]
    
    if new_games.empty:
        print("✅ Games 表已是最新，無需更新。")
        conn.close()
        return

    print(f"🚀 發現 {len(new_games)} 場新比賽，準備寫入...")
    
    data_to_insert = []
    for _, row in new_games.iterrows():
        data_to_insert.append((
            row['GAME_ID'], row['GAME_DATE'], row['SEASON_YEAR'], row['SEASON_TYPE'],
            row['home_team'], row['away_team'], row['home_score'], row['away_score']
        ))

    c.executemany('''
        INSERT OR IGNORE INTO games 
        (game_id, date, season, game_type, home_team, away_team, home_score, away_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', data_to_insert)
    
    conn.commit()
    print(f"✅ 成功寫入 {len(new_games)} 場新比賽！")
    conn.close()

if __name__ == "__main__":
    init_games_table()