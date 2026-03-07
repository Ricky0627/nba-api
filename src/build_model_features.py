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
                df['TEAM_ID'] = df['TEAM_ID'].astype(int)

    base_cols = ['GAME_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'MATCHUP', 'GAME_DATE', 'SEASON_YEAR', 'WL']
    
    avail_adv_cols = [c for c in ['PACE', 'DEF_RATING', 'NET_RATING', 'OFF_RATING', 'TS_PCT', 'EFG_PCT', 'TM_TOV_PCT', 'OREB_PCT', 'PIE'] if c in df_adv.columns]
    
    df_master = df_base[base_cols].copy()
    df_master = df_master.merge(df_adv[['GAME_ID', 'TEAM_ID'] + avail_adv_cols], on=['GAME_ID', 'TEAM_ID'], how='left')

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
        cols_to_use = [c for c in pbp_df.columns if c not in ['TEAM_ABBREVIATION', 'GAME_DATE', 'SEASON_YEAR', 'MATCHUP', 'WL']] 
        df_master = df_master.merge(pbp_df[cols_to_use], on=['GAME_ID', 'TEAM_ID'], how='left')

    if 'Q1_PTS' in df_master.columns and 'Q3_PTS' in df_master.columns:
        df_master['Q1_Q3_GAP'] = df_master['Q1_PTS'] - df_master['Q3_PTS']

    df_master['GAME_DATE'] = pd.to_datetime(df_master['GAME_DATE'].str[:10])
    df_master = df_master.sort_values(['TEAM_ID', 'GAME_DATE']).reset_index(drop=True)
    df_master = df_master.fillna(0)
    
    return df_master

def engineer_rolling_features(df):
    print("⏳ 2. 嚴格防洩漏：計算時間切片與衍生特徵...")
    
    target_metrics = [
        'PACE', 'DEF_RATING', 'NET_RATING', 'OFF_RATING', 'TS_PCT', 'EFG_PCT', 'TM_TOV_PCT', 'OREB_PCT', 'PIE',
        'PCT_PTS_3PT', 'PCT_PTS_PAINT', 'PCT_AST_FGM', 'FTA_RATE', 
        'CONTESTED_SHOTS', 'LOOSE_BALLS_RECOVERED', 'CHARGES_DRAWN', 'SCREEN_ASSISTS',
        'MID_FREQ', 'RIM_FREQ', 'MOREYBALL_INDEX', 
        'CLUTCH_TS_PCT', 'CLUTCH_TOV_PCT', 'RUNS_10_0_COUNT', 'MAX_UNANSWERED_RUN', 'RUN_DEFICIT_RECOVERY_RATE',
        'LIVE_TOV_PCT', 'Q1_Q3_GAP'
    ]
    
    df.columns = [c.upper() for c in df.columns]
    metrics = [c for c in target_metrics if c in df.columns]
    
    rolling_features = {}
    
    for col in metrics:
        group = df.groupby(['TEAM_ID', 'SEASON_YEAR'])[col]
        rolling_features[f'{col}_S2D'] = group.transform(lambda x: x.shift(1).expanding(min_periods=1).mean())
        for n in [3, 5, 10]:
            rolling_features[f'{col}_L{n}'] = group.transform(lambda x: x.shift(1).rolling(n, min_periods=1).mean())
            
    df = pd.concat([df, pd.DataFrame(rolling_features)], axis=1)
    
    other_feats = {}
    
    if 'OFF_RATING' in df.columns:
        other_feats['OFF_RATING_L10_STD'] = df.groupby(['TEAM_ID', 'SEASON_YEAR'])['OFF_RATING'].transform(
            lambda x: x.shift(1).rolling(10, min_periods=3).std()
        )
    if 'OFF_RATING_L5' in df.columns and 'OFF_RATING_S2D' in df.columns:
        other_feats['EFFICIENCY_TREND'] = df['OFF_RATING_L5'] - df['OFF_RATING_S2D']
    
    other_feats['REST_DAYS'] = df.groupby('TEAM_ID')['GAME_DATE'].diff().dt.days
    other_feats['IS_B2B'] = (other_feats['REST_DAYS'] == 1).astype(int)
    
    df['IS_AWAY'] = df['MATCHUP'].str.contains('@').astype(int)
    other_feats['AWAY_STREAK'] = df.groupby(['TEAM_ID', (df['IS_AWAY'] == 0).cumsum()])['IS_AWAY'].cumsum()

    df = pd.concat([df, pd.DataFrame(other_feats)], axis=1)
    
    # 確保所有特徵名稱都是大寫
    df.columns = [c.upper() for c in df.columns]
    return df

def build_final_master_table(df_features):
    print("🥞 3. 正在縫合最終的機器學習特徵寬表 (全大寫對齊)...")
    
    games_raw = get_merged_dataframe("games")
    games_raw.columns = [c.upper() for c in games_raw.columns]
    
    final_games = games_raw[['GAME_ID', 'DATE', 'SEASON', 'HOME_TEAM', 'AWAY_TEAM', 'HOME_SCORE', 'AWAY_SCORE', 'TW_SPREAD_SCORE']].copy()
    final_games['GAME_ID'] = final_games['GAME_ID'].astype(str).str.zfill(10)
    
    df_features.columns = [c.upper() for c in df_features.columns]
    df_features['GAME_ID'] = df_features['GAME_ID'].astype(str).str.zfill(10)
    feature_cols = [c for c in df_features.columns if '_L' in c or '_S2D' in c or 'TREND' in c or 'STD' in c or 'REST' in c or 'B2B' in c or 'STREAK' in c]
    
    feats_subset = df_features[['GAME_ID', 'TEAM_ABBREVIATION'] + feature_cols]
    
    final_df = final_games.merge(
        feats_subset, 
        left_on=['GAME_ID', 'HOME_TEAM'], 
        right_on=['GAME_ID', 'TEAM_ABBREVIATION'], 
        how='inner' 
    )
    final_df = final_df.merge(
        feats_subset, 
        left_on=['GAME_ID', 'AWAY_TEAM'], 
        right_on=['GAME_ID', 'TEAM_ABBREVIATION'], 
        how='inner', 
        suffixes=('_HOME', '_AWAY')
    )

    cols_to_drop = [c for c in final_df.columns if c.startswith('TEAM_ABBREVIATION')]
    final_df = final_df.drop(columns=cols_to_drop)
    
    print("   🚑 正在整合傷兵折損特徵 (MISSING_STATS)...")
    INJURY_CSV = 'data/nba_advanced_injury_features.csv'
    if os.path.exists(INJURY_CSV):
        injury_df = pd.read_csv(INJURY_CSV)
        injury_df.columns = [c.upper() for c in injury_df.columns]
        injury_df['GAME_ID'] = injury_df['GAME_ID'].astype(str).str.zfill(10)
        
        missing_cols = [c for c in injury_df.columns if 'MISSING' in c]
        if missing_cols:
            injury_subset = injury_df[['GAME_ID'] + missing_cols]
            final_df = final_df.merge(injury_subset, on='GAME_ID', how='left')

    final_df['PLUS_MINUS'] = final_df['HOME_SCORE'] - final_df['AWAY_SCORE']
    final_df = final_df.fillna(0)
    
    final_df.rename(columns={'DATE': 'GAME_DATE', 'SEASON': 'SEASON_YEAR'}, inplace=True)
    
    # 確保輸出表頭全大寫
    final_df.columns = [c.upper() for c in final_df.columns]

    print("   ✂️ 正在進行終極特徵瘦身 (只保留神聯軍需要的欄位)...")
    
    ALL_MODELS = [
        {"features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L10_HOME', 'PCT_PTS_PAINT_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'OREB_PCT_L10_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'MID_FREQ_L5_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'FTA_RATE_L10_AWAY']},
        {"features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_PAINT_L5_HOME', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'MAX_UNANSWERED_RUN_L5_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'PACE_S2D_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'TM_TOV_PCT_S2D_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'SCREEN_ASSISTS_L5_AWAY', 'DEF_RATING_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME']},
        {"features": ['CONTESTED_SHOTS_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'FTA_RATE_S2D_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'MAX_UNANSWERED_RUN_L5_HOME', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'CLUTCH_TOV_PCT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'MAX_UNANSWERED_RUN_L3_AWAY', 'TS_PCT_L10_HOME', 'SCREEN_ASSISTS_L5_AWAY', 'PCT_PTS_3PT_L10_AWAY', 'Q1_Q3_GAP_L10_HOME']},
        {"features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L10_HOME', 'PCT_PTS_PAINT_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'OREB_PCT_L10_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'MID_FREQ_L5_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'FTA_RATE_L10_AWAY']},
        {"features": ['CONTESTED_SHOTS_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'FTA_RATE_S2D_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'MAX_UNANSWERED_RUN_L5_HOME', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'CLUTCH_TOV_PCT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'MAX_UNANSWERED_RUN_L3_AWAY', 'TS_PCT_L10_HOME', 'SCREEN_ASSISTS_L5_AWAY', 'PCT_PTS_3PT_L10_AWAY', 'Q1_Q3_GAP_L10_HOME']},
        {"features": ['CONTESTED_SHOTS_L10_AWAY', 'PIE_L5_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'DEF_RATING_L5_AWAY', 'AWAY_STREAK_HOME', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'NET_RATING_L10_HOME', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'CONTESTED_SHOTS_S2D_HOME', 'LOOSE_BALLS_RECOVERED_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L10_HOME', 'PACE_L5_HOME', 'DEF_RATING_S2D_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_S2D_HOME', 'RIM_FREQ_L3_AWAY', 'MID_FREQ_L10_AWAY', 'PCT_PTS_3PT_L10_AWAY', 'AWAY_STREAK_AWAY', 'CHARGES_DRAWN_L3_AWAY']},
        {"features": ['CONTESTED_SHOTS_L10_AWAY', 'PIE_L5_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'DEF_RATING_L5_AWAY', 'PACE_L10_HOME', 'Q1_Q3_GAP_L3_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MOREYBALL_INDEX_L5_HOME', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'CONTESTED_SHOTS_S2D_HOME', 'DEF_RATING_L10_AWAY', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'MOREYBALL_INDEX_L3_AWAY', 'MID_FREQ_L10_AWAY', 'PCT_PTS_3PT_L10_AWAY']},
        {"features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_L3_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'IS_B2B_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'AWAY_STREAK_HOME', 'CLUTCH_TOV_PCT_L3_AWAY', 'PACE_L10_HOME', 'MISSING_MIN_SUM', 'MISSING_USG_PCT_SUM_OPP', 'MISSING_PTS_SUM_OPP']},
        {"features": ['CONTESTED_SHOTS_L10_AWAY', 'PIE_L5_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'DEF_RATING_L5_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'CONTESTED_SHOTS_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'CLUTCH_TOV_PCT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY']},
        {"features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L10_HOME', 'PCT_PTS_PAINT_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'OREB_PCT_L10_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'MID_FREQ_L5_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'FTA_RATE_L10_AWAY', 'MISSING_PIE_SUM', 'MISSING_DEF_RATING_SUM']},
        {"features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L10_HOME', 'PCT_PTS_PAINT_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'OREB_PCT_L10_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'MID_FREQ_L5_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'FTA_RATE_L10_AWAY', 'MISSING_DEF_RATING_SUM', 'MISSING_PIE_SUM_OPP']},
        {"features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L10_HOME', 'PCT_PTS_PAINT_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'OREB_PCT_L10_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'MID_FREQ_L5_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'FTA_RATE_L10_AWAY', 'MISSING_DEF_RATING_SUM']},
        {"features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'CONTESTED_SHOTS_L10_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'SCREEN_ASSISTS_S2D_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'PACE_L10_HOME', 'MISSING_PIE_SUM', 'MISSING_EFF_SUM']},
        {"features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'CONTESTED_SHOTS_L10_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'SCREEN_ASSISTS_S2D_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'PACE_L10_HOME', 'MISSING_MIN_SUM', 'MISSING_USG_PCT_SUM']},
        {"features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'CONTESTED_SHOTS_L10_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'SCREEN_ASSISTS_S2D_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'PACE_L10_HOME', 'MISSING_MIN_SUM', 'MISSING_EFF_SUM']},
        {"features": ['EFFICIENCY_TREND_AWAY', 'OFF_RATING_L10_STD_AWAY', 'PCT_PTS_3PT_L3_HOME', 'CHARGES_DRAWN_L5_AWAY', 'DEF_RATING_L5_AWAY', 'PACE_L10_HOME', 'Q1_Q3_GAP_L5_HOME', 'PCT_PTS_3PT_L10_HOME', 'Q1_Q3_GAP_S2D_HOME', 'CLUTCH_TS_PCT_L3_HOME', 'MID_FREQ_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'TS_PCT_L10_AWAY', 'IS_B2B_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'DEF_RATING_S2D_AWAY', 'PCT_AST_FGM_S2D_HOME']},
        {"features": ['CHARGES_DRAWN_L5_AWAY', 'PCT_PTS_3PT_L3_HOME', 'DEF_RATING_L5_AWAY', 'Q1_Q3_GAP_L5_HOME', 'PACE_L10_HOME', 'Q1_Q3_GAP_S2D_HOME', 'FTA_RATE_L10_AWAY', 'MID_FREQ_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'TS_PCT_L10_AWAY', 'IS_B2B_AWAY', 'PCT_AST_FGM_S2D_HOME']},
        {"features": ['PACE_L3_AWAY', 'Q1_Q3_GAP_L10_HOME', 'DEF_RATING_L3_AWAY', 'PCT_PTS_3PT_L3_HOME', 'PCT_AST_FGM_L5_HOME', 'AWAY_STREAK_HOME', 'Q1_Q3_GAP_L5_HOME', 'PACE_L10_HOME', 'Q1_Q3_GAP_S2D_HOME', 'PACE_S2D_HOME', 'CLUTCH_TS_PCT_L3_HOME', 'FTA_RATE_L10_AWAY', 'MID_FREQ_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'TS_PCT_L10_AWAY', 'IS_B2B_AWAY', 'PCT_AST_FGM_S2D_HOME']},
        {"features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_L3_AWAY', 'TS_PCT_L10_HOME', 'DEF_RATING_L5_AWAY', 'PCT_AST_FGM_L5_HOME', 'PCT_PTS_3PT_L3_HOME', 'NET_RATING_L10_HOME', 'CHARGES_DRAWN_L5_AWAY', 'SCREEN_ASSISTS_L10_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'PACE_L5_HOME', 'TM_TOV_PCT_S2D_HOME', 'CHARGES_DRAWN_L10_HOME', 'DEF_RATING_S2D_HOME', 'CLUTCH_TOV_PCT_L3_AWAY', 'PACE_L10_HOME', 'MISSING_PIE_SUM', 'MISSING_PTS_SUM', 'MISSING_DEF_RATING_SUM_OPP']},
        {"features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_L3_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'IS_B2B_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'AWAY_STREAK_HOME', 'CLUTCH_TOV_PCT_L3_AWAY', 'PACE_L10_HOME', 'MISSING_MIN_SUM', 'MISSING_USG_PCT_SUM_OPP', 'MISSING_PTS_SUM_OPP']},
        {"features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'CONTESTED_SHOTS_L10_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'SCREEN_ASSISTS_S2D_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'PACE_L10_HOME']},
        {"features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_PAINT_L5_HOME', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'MAX_UNANSWERED_RUN_L5_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'PACE_S2D_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'TM_TOV_PCT_S2D_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'SCREEN_ASSISTS_L5_AWAY', 'DEF_RATING_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'MISSING_PIE_SUM', 'MISSING_USG_PCT_SUM_OPP']},
        {"features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'CONTESTED_SHOTS_L10_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'SCREEN_ASSISTS_S2D_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'PACE_L10_HOME', 'MISSING_EFF_SUM']},
        {"features": ['DEF_RATING_L5_HOME', 'Q1_Q3_GAP_L5_HOME', 'RUN_DEFICIT_RECOVERY_RATE_L5_HOME', 'REST_DAYS_HOME', 'Q1_Q3_GAP_L10_HOME', 'PACE_L10_HOME', 'Q1_Q3_GAP_S2D_HOME', 'CLUTCH_TS_PCT_L3_HOME', 'MID_FREQ_L10_AWAY', 'IS_B2B_AWAY', 'CHARGES_DRAWN_L5_AWAY', 'Q1_Q3_GAP_L3_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'TS_PCT_L10_AWAY', 'PCT_AST_FGM_S2D_HOME', 'DEF_RATING_L3_AWAY', 'PCT_PTS_3PT_L3_HOME', 'RUNS_10_0_COUNT_L3_HOME', 'EFFICIENCY_TREND_AWAY']}
    ]

    all_needed_features = set(['GAME_ID', 'GAME_DATE', 'SEASON_YEAR', 'HOME_TEAM', 'AWAY_TEAM', 'TW_SPREAD_SCORE', 'PLUS_MINUS'])
    for m in ALL_MODELS:
        all_needed_features.update(m['features'])
        
    cols_to_keep = [c for c in final_df.columns if c in all_needed_features]
    final_df = final_df[cols_to_keep]

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    final_df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n✅ 終極大表大功告成！已精簡並匯出至: {OUTPUT_CSV}")

if __name__ == "__main__":
    print("🚀 啟動 NBA 終極特徵工程引擎 (Feature Store Builder)")
    df_logs = load_and_merge_team_logs()
    df_features = engineer_rolling_features(df_logs)
    build_final_master_table(df_features)