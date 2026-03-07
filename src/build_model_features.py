import pandas as pd
import numpy as np
import os
from datetime import datetime
from prepare_data import get_merged_dataframe

# ==========================================
# ⚙️ 終極機器學習特徵工廠 (NBA Master Feature Factory)
# ==========================================
OUTPUT_FILE = "data/ml_features_master.csv"

# 1️⃣ 定義要滾動計算的欄位清單
COLS_BASE = ['PTS', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'FG_PCT', 'FG3_PCT']
COLS_ADV = ['OFF_RATING', 'DEF_RATING', 'NET_RATING', 'TS_PCT', 'EFG_PCT', 'PACE', 'PIE']
COLS_4F = ['EFG_PCT', 'TM_TOV_PCT', 'OREB_PCT', 'FTA_RATE']
COLS_HUSTLE = ['CONTESTED_SHOTS', 'DEFLECTIONS', 'CHARGES_DRAWN', 'SCREEN_ASSISTS', 'LOOSE_BALLS_RECOVERED']
COLS_SCORING = ['PCT_PTS_3PT', 'PCT_PTS_PAINT', 'PCT_AST_FGM']
COLS_PBP = [
    'Clutch_TS_pct', 'Clutch_TOV_pct', 
    'Moreyball_Index', 'Rim_FREQ', 'Mid_FREQ', 
    'Live_TOV_pct', 
    'Max_Unanswered_Run', 'Run_Deficit_Recovery_Rate', 'Runs_10_0_Count'
]

def calculate_elo(df_games):
    """計算跨賽季 ELO Rating (考慮主場優勢與賽季回歸)"""
    elo_dict = {}
    elo_history = []
    
    K = 20
    HOME_ADVANTAGE = 100
    INITIAL_ELO = 1505
    
    df_games = df_games.sort_values('date').reset_index(drop=True)
    last_season = None
    
    for _, game in df_games.iterrows():
        current_season = game['season']
        h_tid = game['home_team']
        a_tid = game['away_team']
        
        # 賽季切換回歸 (Mean Reversion)
        if last_season and current_season != last_season:
            for tid in elo_dict:
                elo_dict[tid] = 0.75 * elo_dict[tid] + 0.25 * INITIAL_ELO
        last_season = current_season
        
        h_elo = elo_dict.get(h_tid, INITIAL_ELO)
        a_elo = elo_dict.get(a_tid, INITIAL_ELO)
        
        elo_history.append({
            'GAME_ID': game['game_id'],
            'HOME_ELO_PRE': h_elo,
            'AWAY_ELO_PRE': a_elo
        })
        
        h_exp = 1 / (1 + 10 ** ((a_elo - (h_elo + HOME_ADVANTAGE)) / 400))
        a_exp = 1 - h_exp
        
        h_act = 1 if game['home_score'] > game['away_score'] else 0
        a_act = 1 - h_act
        
        elo_dict[h_tid] = h_elo + K * (h_act - h_exp)
        elo_dict[a_tid] = a_elo + K * (a_act - a_exp)
        
    return pd.DataFrame(elo_history)

def build_master_features():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    print("🔌 正在從 GitHub 資料庫讀取數據...")
    
    # --- 讀取所有必要表 ---
    base = get_merged_dataframe("boxscore_base")
    adv = get_merged_dataframe("boxscore_advanced")
    ff = get_merged_dataframe("boxscore_four_factors")
    hustle = get_merged_dataframe("boxscore_hustle")
    scoring = get_merged_dataframe("boxscore_scoring")
    clutch = get_merged_dataframe("team_features_clutch")
    shot = get_merged_dataframe("team_features_shot_profile")
    tov = get_merged_dataframe("team_features_turnover")
    momentum = get_merged_dataframe("team_features_momentum")
    quarterly = get_merged_dataframe("team_features_quarterly")
    
    games_raw = get_merged_dataframe("games")
    # 將 games_raw 欄位轉小寫以完全對齊你的程式碼邏輯
    games_raw.columns = [c.lower() for c in games_raw.columns]
    
    # 確保 ID 格式統一
    for df in [base, adv, ff, hustle, scoring, clutch, shot, tov, momentum, quarterly]:
        if not df.empty:
            if 'GAME_ID' in df.columns:
                df['GAME_ID'] = df['GAME_ID'].astype(str).str.zfill(10)
            if 'TEAM_ID' in df.columns:
                df['TEAM_ID'] = df['TEAM_ID'].astype(int)

    # --- 大合併 (包含 TEAM_ABBREVIATION 以解決字串合併錯誤) ---
    df_master = base[['GAME_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'MATCHUP', 'GAME_DATE', 'SEASON_YEAR', 'WL']].copy()
    df_master = df_master.merge(adv[['GAME_ID', 'TEAM_ID'] + COLS_ADV], on=['GAME_ID', 'TEAM_ID'], how='left')
    df_master = df_master.merge(ff[['GAME_ID', 'TEAM_ID'] + COLS_4F], on=['GAME_ID', 'TEAM_ID'], how='left', suffixes=('', '_ff'))
    df_master = df_master.merge(hustle[['GAME_ID', 'TEAM_ID'] + COLS_HUSTLE], on=['GAME_ID', 'TEAM_ID'], how='left')
    df_master = df_master.merge(scoring[['GAME_ID', 'TEAM_ID'] + COLS_SCORING], on=['GAME_ID', 'TEAM_ID'], how='left')
    df_master = df_master.merge(clutch[['GAME_ID', 'TEAM_ID', 'Clutch_TS_pct', 'Clutch_TOV_pct']], on=['GAME_ID', 'TEAM_ID'], how='left')
    df_master = df_master.merge(shot[['GAME_ID', 'TEAM_ID', 'Moreyball_Index', 'Rim_FREQ', 'Mid_FREQ']], on=['GAME_ID', 'TEAM_ID'], how='left')
    df_master = df_master.merge(tov[['GAME_ID', 'TEAM_ID', 'Live_TOV_pct']], on=['GAME_ID', 'TEAM_ID'], how='left')
    df_master = df_master.merge(momentum[['GAME_ID', 'TEAM_ID'] + ['Max_Unanswered_Run', 'Run_Deficit_Recovery_Rate', 'Runs_10_0_Count']], on=['GAME_ID', 'TEAM_ID'], how='left')
    df_master = df_master.merge(quarterly[['GAME_ID', 'TEAM_ID', 'Q1_PTS', 'Q3_PTS']], on=['GAME_ID', 'TEAM_ID'], how='left')

    df_master['Q1_Q3_Gap'] = df_master['Q1_PTS'] - df_master['Q3_PTS']
    
    # 確保按時間與球隊排序，這是防洩漏的核心！
    df_master['GAME_DATE'] = pd.to_datetime(df_master['GAME_DATE'])
    df_master = df_master.sort_values(['TEAM_ID', 'GAME_DATE']).reset_index(drop=True)
    df_master = df_master.fillna(0)

    # 2️⃣ 執行滾動平均特徵 (使用字典收集，消除 PerformanceWarning)
    print("🧪 正在計算滾動平均特徵 (S2D, L10, L5, L3) 🚀加速中...")
    ALL_ROLLING_COLS = list(set(COLS_BASE + COLS_ADV + COLS_4F + COLS_HUSTLE + COLS_SCORING + COLS_PBP + ['Q1_Q3_Gap']))
    
    rolling_features = {}
    for col in ALL_ROLLING_COLS:
        if col not in df_master.columns: continue
        group = df_master.groupby(['TEAM_ID', 'SEASON_YEAR'])[col]
        # 賽季平均
        rolling_features[f'{col}_S2D'] = group.transform(lambda x: x.shift(1).expanding().mean())
        # 滾動平均
        for n in [10, 5, 3]:
            rolling_features[f'{col}_L{n}'] = group.transform(lambda x: x.shift(1).rolling(n, min_periods=1).mean())

    # 一次性合併所有滾動特徵 (解決碎片化問題)
    df_master = pd.concat([df_master, pd.DataFrame(rolling_features)], axis=1)

    # 3️⃣ 趨勢、穩定度、體力與賽程
    print("📈 正在計算戰力趨勢、穩定度與體力賽程...")
    other_feats = {}
    
    # 穩定度 (標準差)
    other_feats['OFF_RATING_L10_STD'] = df_master.groupby(['TEAM_ID', 'SEASON_YEAR'])['OFF_RATING'].transform(
        lambda x: x.shift(1).rolling(10, min_periods=3).std()
    )
    # 近期狀態趨勢
    other_feats['Efficiency_Trend'] = df_master['OFF_RATING_L5'] - df_master['OFF_RATING_S2D']
    
    # 體力與客場之旅
    other_feats['Rest_Days'] = df_master.groupby('TEAM_ID')['GAME_DATE'].diff().dt.days
    other_feats['Is_B2B'] = (other_feats['Rest_Days'] == 1).astype(int)
    
    df_master['Is_Away'] = df_master['MATCHUP'].str.contains('@').astype(int)
    other_feats['Away_Streak'] = df_master.groupby(['TEAM_ID', (df_master['Is_Away'] == 0).cumsum()])['Is_Away'].cumsum()

    # 合併其他特徵
    df_master = pd.concat([df_master, pd.DataFrame(other_feats)], axis=1)

    # 4️⃣ ELO Rating 與對戰歷史
    print("🏰 正在計算 ELO Rating...")
    elo_df = calculate_elo(games_raw)
    elo_df['GAME_ID'] = elo_df['GAME_ID'].astype(str).str.zfill(10)

    # 5️⃣ 轉換為「主客對戰寬表格式」
    print("🥞 正在縫合最終的機器學習特徵寬表...")
    final_games = games_raw[['game_id', 'date', 'season', 'home_team', 'away_team', 'home_score', 'away_score', 'tw_spread_score', 'tw_total_score', 'tw_moneyline_home', 'tw_moneyline_away']].copy()
    final_games['game_id'] = final_games['game_id'].astype(str).str.zfill(10)
    
    # 併入 ELO
    final_games = final_games.merge(elo_df, left_on='game_id', right_on='GAME_ID', how='left').drop(columns=['GAME_ID'])
    
    # 提取我們剛算出的機器學習特徵
    feature_cols = [c for c in df_master.columns if '_L' in c or '_S2D' in c or 'Trend' in c or 'STD' in c or 'Rest' in c or 'Is_B2B' in c or 'Away_Streak' in c]
    
    # ⚠️ 關鍵修復：這裡使用 TEAM_ABBREVIATION 來對齊 games 表裡面的字串 (如 'CLE')
    feats_subset = df_master[['GAME_ID', 'TEAM_ABBREVIATION'] + feature_cols]
    
    # 主隊特徵
    final_df = final_games.merge(
        feats_subset, 
        left_on=['game_id', 'home_team'], 
        right_on=['GAME_ID', 'TEAM_ABBREVIATION'], 
        how='inner' # 用 inner 過濾掉沒有數據的早期比賽
    )
    # 客隊特徵
    final_df = final_df.merge(
        feats_subset, 
        left_on=['game_id', 'away_team'], 
        right_on=['GAME_ID', 'TEAM_ABBREVIATION'], 
        how='inner', 
        suffixes=('_HOME', '_AWAY')
    )

    # 刪除多餘的合併鍵
    cols_to_drop = [c for c in final_df.columns if c.startswith('GAME_ID') or c.startswith('TEAM_ABBREVIATION')]
    final_df = final_df.drop(columns=cols_to_drop)

    # 6️⃣ 匯出
    print(f"💾 正在匯出至: {OUTPUT_FILE}")
    final_df.to_csv(OUTPUT_FILE, index=False)
    
    print(f"🎉 特徵提煉全部完成！總行數: {len(final_df)}，總特徵維度: {len(final_df.columns)}")

if __name__ == "__main__":
    build_master_features()