import pandas as pd
import numpy as np
import os
import warnings

from prepare_data import get_merged_dataframe

warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ 設定區
# ==========================================
OUTPUT_CSV = 'data/ml_features_master.csv'
INJURY_CSV = 'data/nba_advanced_injury_features.csv'

def load_and_merge_team_logs():
    print("📥 1. 載入並合併所有基礎與進階數據庫...")
    
    df_adv = get_merged_dataframe("boxscore_advanced")
    df_base = get_merged_dataframe("boxscore_base")
    df_score = get_merged_dataframe("boxscore_scoring")
    df_four = get_merged_dataframe("boxscore_four_factors")
    df_hustle = get_merged_dataframe("boxscore_hustle")
    
    df_clutch = get_merged_dataframe("team_features_clutch")
    df_shot = get_merged_dataframe("team_features_shot_profile")
    df_tov = get_merged_dataframe("team_features_turnover")
    df_mom = get_merged_dataframe("team_features_momentum")
    df_qtr = get_merged_dataframe("team_features_quarterly")

    all_dfs = [df_adv, df_base, df_score, df_four, df_hustle, df_clutch, df_shot, df_tov, df_mom, df_qtr]
    for df in all_dfs:
        if not df.empty:
            df.columns = [c.upper() for c in df.columns]
            if 'GAME_ID' in df.columns:
                df['GAME_ID'] = df['GAME_ID'].astype(str).str.zfill(10)
            if 'TEAM_ID' in df.columns:
                df['TEAM_ID'] = df['TEAM_ID'].astype(str)

    base_cols = ['GAME_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'GAME_DATE', 'SEASON_YEAR', 'MATCHUP']
    
    avail_adv_cols = [c for c in ['PACE', 'DEF_RATING', 'NET_RATING', 'OFF_RATING', 'TS_PCT', 'EFG_PCT', 'TM_TOV_PCT', 'OREB_PCT', 'PIE'] if c in df_adv.columns]
    df_master = df_adv[base_cols + avail_adv_cols].copy()

    score_cols = [c for c in ['PCT_PTS_3PT', 'PCT_PTS_PAINT', 'PCT_AST_FGM'] if c in df_score.columns]
    if score_cols:
        df_master = df_master.merge(df_score[['GAME_ID', 'TEAM_ID'] + score_cols], on=['GAME_ID', 'TEAM_ID'], how='left')

    if 'FTA_RATE' in df_four.columns:
        df_master = df_master.merge(df_four[['GAME_ID', 'TEAM_ID', 'FTA_RATE']], on=['GAME_ID', 'TEAM_ID'], how='left')

    hustle_cols = [c for c in ['CONTESTED_SHOTS', 'LOOSE_BALLS_RECOVERED', 'CHARGES_DRAWN', 'SCREEN_ASSISTS'] if c in df_hustle.columns]
    if hustle_cols:
        df_master = df_master.merge(df_hustle[['GAME_ID', 'TEAM_ID'] + hustle_cols], on=['GAME_ID', 'TEAM_ID'], how='left')

    for pbp_df in [df_clutch, df_shot, df_tov, df_mom, df_qtr]:
        if pbp_df.empty: continue
        cols_to_use = [c for c in pbp_df.columns if c not in ['TEAM_ABBREVIATION', 'GAME_DATE', 'SEASON_YEAR', 'MATCHUP']] 
        df_master = df_master.merge(pbp_df[cols_to_use], on=['GAME_ID', 'TEAM_ID'], how='left')

    df_master['GAME_DATE'] = pd.to_datetime(df_master['GAME_DATE'].str[:10])
    df_master = df_master.sort_values(['TEAM_ID', 'GAME_DATE']).reset_index(drop=True)
    
    df_master = df_master.fillna(0)
    
    return df_master

def engineer_rolling_features(df):
    print("⏳ 2. 嚴格防洩漏：計算時間切片與衍生特徵 (🚨已還原 Transform 確保絕對對齊)...")
    
    target_metrics = [
        'PACE', 'DEF_RATING', 'NET_RATING', 'OFF_RATING', 'TS_PCT', 'EFG_PCT', 'TM_TOV_PCT', 'OREB_PCT', 'PIE',
        'PCT_PTS_3PT', 'PCT_PTS_PAINT', 'PCT_AST_FGM', 'FTA_RATE', 
        'CONTESTED_SHOTS', 'LOOSE_BALLS_RECOVERED', 'CHARGES_DRAWN', 'SCREEN_ASSISTS',
        'MID_FREQ', 'RIM_FREQ', 'MOREYBALL_INDEX', 
        'CLUTCH_TS_PCT', 'CLUTCH_TOV_PCT', 'RUNS_10_0_COUNT', 'MAX_UNANSWERED_RUN', 'RUN_DEFICIT_RECOVERY_RATE',
        'LIVE_TOV_PCT'
    ]
    
    df.columns = [c.upper() for c in df.columns]
    metrics = [c for c in target_metrics if c in df.columns]
    
    if 'Q1_PTS' in df.columns and 'Q3_PTS' in df.columns:
        df['Q1_Q3_GAP'] = df['Q1_PTS'] - df['Q3_PTS']
        metrics.append('Q1_Q3_GAP')
        
    # ⚠️ 最關鍵的一步：確保資料完全依據球隊與時間排序，後續 transform 才能完美對齊
    df = df.sort_values(['TEAM_ID', 'GAME_DATE']).reset_index(drop=True)
    
    rolling_features = {}
    
    # 🚨 全面改回原始腳本的 .transform()，徹底消滅 Index 錯位 Bug！
    for col in metrics:
        group = df.groupby(['TEAM_ID', 'SEASON_YEAR'])[col]
        rolling_features[f'{col}_S2D'] = group.transform(lambda x: x.shift(1).expanding(min_periods=1).mean())
        for n in [3, 5, 10]:
            rolling_features[f'{col}_L{n}'] = group.transform(lambda x: x.shift(1).rolling(n, min_periods=1).mean())
            
    df = pd.concat([df, pd.DataFrame(rolling_features)], axis=1)
    
    # 🚨 修正 STD 啟動門檻與對齊
    if 'OFF_RATING' in df.columns:
        df['OFF_RATING_L10_STD'] = df.groupby(['TEAM_ID', 'SEASON_YEAR'])['OFF_RATING'].transform(
            lambda x: x.shift(1).rolling(10, min_periods=3).std()
        )
    
    # 🚨 修正 Trend 邏輯 (改回進攻效率)
    if 'OFF_RATING_L5' in df.columns and 'OFF_RATING_S2D' in df.columns:
        df['EFFICIENCY_TREND'] = df['OFF_RATING_L5'] - df['OFF_RATING_S2D']
    
    # 體力與賽程 (完美對齊)
    df['REST_DAYS'] = df.groupby('TEAM_ID')['GAME_DATE'].diff().dt.days
    df['IS_B2B'] = (df['REST_DAYS'] == 1).astype(int)
    
    if 'MATCHUP' in df.columns:
        df['IS_AWAY'] = df['MATCHUP'].str.contains('@').astype(int)
    else:
        df['IS_AWAY'] = 0
        
    df['AWAY_STREAK'] = df.groupby(['TEAM_ID', (df['IS_AWAY'] == 0).cumsum()])['IS_AWAY'].cumsum()
    
    return df

def build_final_master_table(df_features):
    print("🧩 3. 將特徵合併至傷兵大表 (Base Table)...")
    
    if not os.path.exists(INJURY_CSV):
        print(f"❌ 嚴重錯誤：找不到 {INJURY_CSV}！請先確保 generate_injury.py 已執行。")
        return
        
    final_df = pd.read_csv(INJURY_CSV)
    final_df['game_id'] = final_df['game_id'].astype(str).str.zfill(10)
    
    df_features['GAME_ID'] = df_features['GAME_ID'].astype(str).str.zfill(10)
    
    keep_cols = ['GAME_ID', 'TEAM_ABBREVIATION', 'REST_DAYS', 'IS_B2B', 'AWAY_STREAK', 'EFFICIENCY_TREND', 'OFF_RATING_L10_STD']
    keep_cols = [c for c in keep_cols if c in df_features.columns]
    keep_cols += [c for c in df_features.columns if c.endswith(('_L3', '_L5', '_L10', '_S2D'))]
    df_feat_clean = df_features[keep_cols].copy()
    
    # ==== 對接主隊 (HOME) ====
    home_feats = df_feat_clean.copy()
    home_feats.columns = [f"HOME_{c}" if c not in ['GAME_ID', 'TEAM_ABBREVIATION'] else c for c in home_feats.columns]
    final_df = final_df.merge(home_feats, left_on=['game_id', 'home_team'], right_on=['GAME_ID', 'TEAM_ABBREVIATION'], how='left')
    
    if 'GAME_ID' in final_df.columns: final_df = final_df.drop(columns=['GAME_ID'])
    if 'TEAM_ABBREVIATION' in final_df.columns: final_df = final_df.drop(columns=['TEAM_ABBREVIATION'])

    # ==== 對接客隊 (AWAY) ====
    away_feats = df_feat_clean.copy()
    away_feats.columns = [f"AWAY_{c}" if c not in ['GAME_ID', 'TEAM_ABBREVIATION'] else c for c in away_feats.columns]
    final_df = final_df.merge(away_feats, left_on=['game_id', 'away_team'], right_on=['GAME_ID', 'TEAM_ABBREVIATION'], how='left')
    
    if 'GAME_ID' in final_df.columns: final_df = final_df.drop(columns=['GAME_ID'])
    if 'TEAM_ABBREVIATION' in final_df.columns: final_df = final_df.drop(columns=['TEAM_ABBREVIATION'])

    # 🔥 神級改造：從資料庫直接提取 Target 需要的歷史賠率與淨勝分
    print("   🔗 正在寫入預測目標 (TW_SPREAD_SCORE & PLUS_MINUS)...")
    
    games_df = get_merged_dataframe("games")
    games_df.columns = [c.upper() for c in games_df.columns]
    if 'TW_SPREAD_SCORE' in games_df.columns:
        odds_df = games_df[['GAME_ID', 'TW_SPREAD_SCORE']].drop_duplicates()
        odds_df['GAME_ID'] = odds_df['GAME_ID'].astype(str).str.zfill(10)
        final_df = final_df.merge(odds_df, left_on='game_id', right_on='GAME_ID', how='left')
        if 'GAME_ID' in final_df.columns: final_df = final_df.drop(columns=['GAME_ID'])
    
    base_df = get_merged_dataframe("boxscore_base")
    base_df.columns = [c.upper() for c in base_df.columns]
    if 'PLUS_MINUS' in base_df.columns and 'MATCHUP' in base_df.columns:
        home_base = base_df[base_df['MATCHUP'].str.contains(' vs. ', na=False)].drop_duplicates('GAME_ID')
        home_base = home_base[['GAME_ID', 'PLUS_MINUS']]
        home_base['GAME_ID'] = home_base['GAME_ID'].astype(str).str.zfill(10)
        final_df = final_df.merge(home_base, left_on='game_id', right_on='GAME_ID', how='left')
        if 'GAME_ID' in final_df.columns: final_df = final_df.drop(columns=['GAME_ID'])

    final_df = final_df.fillna(0)
    final_df.rename(columns={'game_id': 'GAME_ID', 'home_team': 'HOME_TEAM', 'away_team': 'AWAY_TEAM'}, inplace=True)
    
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    final_df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n✅ 終極大表大功告成！已輸出至: {OUTPUT_CSV}")

if __name__ == "__main__":
    print("🚀 啟動 NBA 終極特徵工程引擎 (Feature Store Builder)")
    df_logs = load_and_merge_team_logs()
    df_features = engineer_rolling_features(df_logs)
    build_final_master_table(df_features)