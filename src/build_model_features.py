import pandas as pd
import numpy as np
import os
import warnings

from prepare_data import get_merged_dataframe

warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ 終極機器學習特徵工廠 (GitHub 移植版)
# ==========================================
OUTPUT_CSV = 'data/ml_features_master.csv'

# 1️⃣ 定義要滾動計算的欄位清單 (對齊 Local 邏輯)
COLS_BASE = ['PTS', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'FG_PCT', 'FG3_PCT']
COLS_ADV = ['OFF_RATING', 'DEF_RATING', 'NET_RATING', 'TS_PCT', 'EFG_PCT', 'PACE', 'PIE']
COLS_4F = ['EFG_PCT', 'TM_TOV_PCT', 'OREB_PCT', 'FTA_RATE']
COLS_HUSTLE = ['CONTESTED_SHOTS', 'DEFLECTIONS', 'CHARGES_DRAWN', 'SCREEN_ASSISTS', 'LOOSE_BALLS_RECOVERED']
COLS_SCORING = ['PCT_PTS_3PT', 'PCT_PTS_PAINT', 'PCT_AST_FGM']
COLS_PBP = [
    'CLUTCH_TS_PCT', 'CLUTCH_TOV_PCT', 
    'MOREYBALL_INDEX', 'RIM_FREQ', 'MID_FREQ', 
    'LIVE_TOV_PCT', 
    'MAX_UNANSWERED_RUN', 'RUN_DEFICIT_RECOVERY_RATE', 'RUNS_10_0_COUNT'
]

def build_master_features():
    if not os.path.exists(os.path.dirname(OUTPUT_CSV)): 
        os.makedirs(os.path.dirname(OUTPUT_CSV))
        
    print("🔌 正在從 GitHub CSV 數據庫讀取數據 (完美還原 Local 邏輯)...")
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
    
    # 強制將所有表格轉為大寫，避免因大小寫引發 KeyError
    for df in [base, adv, ff, hustle, scoring, clutch, shot, tov, momentum, quarterly, games_raw]:
        if not df.empty:
            df.columns = [c.upper() for c in df.columns]

    # 確保 ID 格式統一
    for df in [base, adv, ff, hustle, scoring, clutch, shot, tov, momentum, quarterly]:
        if not df.empty:
            df['GAME_ID'] = df['GAME_ID'].astype(str).str.zfill(10)
            df['TEAM_ID'] = df['TEAM_ID'].astype(int)

    # --- 大合併 ---
    df_master = base[['GAME_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'MATCHUP', 'GAME_DATE', 'SEASON_YEAR', 'WL']].copy()
    
    if not adv.empty: df_master = df_master.merge(adv[['GAME_ID', 'TEAM_ID'] + [c for c in COLS_ADV if c in adv.columns]], on=['GAME_ID', 'TEAM_ID'], how='left')
    if not ff.empty: df_master = df_master.merge(ff[['GAME_ID', 'TEAM_ID'] + [c for c in COLS_4F if c in ff.columns]], on=['GAME_ID', 'TEAM_ID'], how='left', suffixes=('', '_FF'))
    if not hustle.empty: df_master = df_master.merge(hustle[['GAME_ID', 'TEAM_ID'] + [c for c in COLS_HUSTLE if c in hustle.columns]], on=['GAME_ID', 'TEAM_ID'], how='left')
    if not scoring.empty: df_master = df_master.merge(scoring[['GAME_ID', 'TEAM_ID'] + [c for c in COLS_SCORING if c in scoring.columns]], on=['GAME_ID', 'TEAM_ID'], how='left')
    if not clutch.empty: df_master = df_master.merge(clutch[['GAME_ID', 'TEAM_ID'] + [c for c in ['CLUTCH_TS_PCT', 'CLUTCH_TOV_PCT'] if c in clutch.columns]], on=['GAME_ID', 'TEAM_ID'], how='left')
    if not shot.empty: df_master = df_master.merge(shot[['GAME_ID', 'TEAM_ID'] + [c for c in ['MOREYBALL_INDEX', 'RIM_FREQ', 'MID_FREQ'] if c in shot.columns]], on=['GAME_ID', 'TEAM_ID'], how='left')
    if not tov.empty: df_master = df_master.merge(tov[['GAME_ID', 'TEAM_ID'] + [c for c in ['LIVE_TOV_PCT'] if c in tov.columns]], on=['GAME_ID', 'TEAM_ID'], how='left')
    if not momentum.empty: df_master = df_master.merge(momentum[['GAME_ID', 'TEAM_ID'] + [c for c in ['MAX_UNANSWERED_RUN', 'RUN_DEFICIT_RECOVERY_RATE', 'RUNS_10_0_COUNT'] if c in momentum.columns]], on=['GAME_ID', 'TEAM_ID'], how='left')
    if not quarterly.empty: df_master = df_master.merge(quarterly[['GAME_ID', 'TEAM_ID'] + [c for c in ['Q1_PTS', 'Q3_PTS'] if c in quarterly.columns]], on=['GAME_ID', 'TEAM_ID'], how='left')

    if 'Q1_PTS' in df_master.columns and 'Q3_PTS' in df_master.columns:
        df_master['Q1_Q3_GAP'] = df_master['Q1_PTS'] - df_master['Q3_PTS']
    
    # 確保按時間與球隊排序，這是防洩漏的核心！
    df_master['GAME_DATE'] = pd.to_datetime(df_master['GAME_DATE'])
    df_master = df_master.sort_values(['TEAM_ID', 'GAME_DATE']).reset_index(drop=True)
    df_master = df_master.fillna(0)

    # 2️⃣ 執行滾動平均特徵
    print("🧪 正在計算滾動平均特徵 (S2D, L10, L5, L3) 🚀加速中...")
    ALL_ROLLING_COLS = list(set(COLS_BASE + COLS_ADV + COLS_4F + COLS_HUSTLE + COLS_SCORING + COLS_PBP + ['Q1_Q3_GAP']))
    
    rolling_features = {}
    for col in ALL_ROLLING_COLS:
        if col not in df_master.columns: continue
        group = df_master.groupby(['TEAM_ID', 'SEASON_YEAR'])[col]
        # 賽季平均
        rolling_features[f'{col}_S2D'] = group.transform(lambda x: x.shift(1).expanding().mean())
        # 滾動平均
        for n in [10, 5, 3]:
            rolling_features[f'{col}_L{n}'] = group.transform(lambda x: x.shift(1).rolling(n, min_periods=1).mean())

    df_master = pd.concat([df_master, pd.DataFrame(rolling_features)], axis=1)

    # 3️⃣ 趨勢、穩定度、體力與賽程
    print("📈 正在計算戰力趨勢、穩定度與體力賽程...")
    other_feats = {}
    
    if 'OFF_RATING' in df_master.columns:
        other_feats['OFF_RATING_L10_STD'] = df_master.groupby(['TEAM_ID', 'SEASON_YEAR'])['OFF_RATING'].transform(
            lambda x: x.shift(1).rolling(10, min_periods=3).std()
        )
    if 'OFF_RATING_L5' in df_master.columns and 'OFF_RATING_S2D' in df_master.columns:
        other_feats['EFFICIENCY_TREND'] = df_master['OFF_RATING_L5'] - df_master['OFF_RATING_S2D']
    
    other_feats['REST_DAYS'] = df_master.groupby('TEAM_ID')['GAME_DATE'].diff().dt.days
    other_feats['IS_B2B'] = (other_feats['REST_DAYS'] == 1).astype(int)
    
    df_master['IS_AWAY'] = df_master['MATCHUP'].str.contains('@').astype(int)
    other_feats['AWAY_STREAK'] = df_master.groupby(['TEAM_ID', (df_master['IS_AWAY'] == 0).cumsum()])['IS_AWAY'].cumsum()

    df_master = pd.concat([df_master, pd.DataFrame(other_feats)], axis=1)

    # 5️⃣ 轉換為「主客對戰寬表格式」
    print("🥞 正在縫合最終的機器學習特徵寬表...")
    games_raw['GAME_ID'] = games_raw['GAME_ID'].astype(str).str.zfill(10)
    
    keep_cols = ['GAME_ID', 'DATE', 'SEASON', 'HOME_TEAM', 'AWAY_TEAM', 'HOME_SCORE', 'AWAY_SCORE', 'TW_SPREAD_SCORE']
    final_games = games_raw[[c for c in keep_cols if c in games_raw.columns]].copy()
    
    feature_cols = [c for c in df_master.columns if '_L' in c or '_S2D' in c or 'TREND' in c or 'STD' in c or 'REST' in c or 'B2B' in c or 'STREAK' in c]
    
    # ⚠️ 關鍵修復：使用 TEAM_ABBREVIATION 對齊
    feats_subset = df_master[['GAME_ID', 'TEAM_ABBREVIATION'] + feature_cols]
    
    # 主隊特徵
    final_df = final_games.merge(
        feats_subset, 
        left_on=['GAME_ID', 'HOME_TEAM'], 
        right_on=['GAME_ID', 'TEAM_ABBREVIATION'], 
        how='inner' 
    )
    # 客隊特徵
    final_df = final_df.merge(
        feats_subset, 
        left_on=['GAME_ID', 'AWAY_TEAM'], 
        right_on=['GAME_ID', 'TEAM_ABBREVIATION'], 
        how='inner', 
        suffixes=('_HOME', '_AWAY')
    )

    cols_to_drop = [c for c in final_df.columns if c.startswith('TEAM_ABBREVIATION')]
    final_df = final_df.drop(columns=cols_to_drop)

    # 🚑 加入傷兵特徵
    print("   🚑 正在整合傷兵折損特徵 (MISSING_STATS)...")
    INJURY_CSV = 'data/nba_advanced_injury_features.csv'
    if os.path.exists(INJURY_CSV):
        injury_df = pd.read_csv(INJURY_CSV)
        injury_df.columns = [c.upper() for c in injury_df.columns]
        injury_df['GAME_ID'] = injury_df['GAME_ID'].astype(str).str.zfill(10)
        missing_cols = [c for c in injury_df.columns if 'MISSING' in c]
        if missing_cols:
            final_df = final_df.merge(injury_df[['GAME_ID'] + missing_cols], on='GAME_ID', how='left')

    if 'HOME_SCORE' in final_df.columns and 'AWAY_SCORE' in final_df.columns:
        final_df['PLUS_MINUS'] = final_df['HOME_SCORE'] - final_df['AWAY_SCORE']
        
    final_df = final_df.fillna(0)
    final_df.rename(columns={'DATE': 'GAME_DATE', 'SEASON': 'SEASON_YEAR'}, inplace=True)
    
    # 🔥 自動變形器：將 _HOME, _AWAY 後綴轉換為 HOME_, AWAY_ 前綴！(完全對齊你的 local 腳本)
    new_cols = []
    for c in final_df.columns:
        if c.endswith('_HOME'):
            new_cols.append('HOME_' + c[:-5])
        elif c.endswith('_AWAY'):
            new_cols.append('AWAY_' + c[:-5])
        else:
            new_cols.append(c)
    final_df.columns = new_cols

    # ✂️ 取我們需要的特徵就好
    print("   ✂️ 正在進行終極特徵瘦身 (只保留神聯軍需要的欄位)...")
    
    ALL_MODELS = [
        {"features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L10', 'AWAY_PCT_PTS_PAINT_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_OREB_PCT_L10', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'AWAY_MID_FREQ_L5', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_PCT_AST_FGM_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_PCT_PTS_3PT_L10', 'HOME_EFFICIENCY_TREND', 'AWAY_FTA_RATE_L10']},
        {"features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_PAINT_L5', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_PCT_AST_FGM_L10', 'HOME_MAX_UNANSWERED_RUN_L5', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'AWAY_PACE_S2D', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_TM_TOV_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_SCREEN_ASSISTS_L5', 'HOME_DEF_RATING_S2D', 'AWAY_PCT_PTS_3PT_L10', 'HOME_EFFICIENCY_TREND']},
        {"features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_EFFICIENCY_TREND', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'AWAY_FTA_RATE_S2D', 'HOME_PCT_PTS_PAINT_L5', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_MAX_UNANSWERED_RUN_L5', 'HOME_PCT_AST_FGM_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_CLUTCH_TOV_PCT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'AWAY_MAX_UNANSWERED_RUN_L3', 'HOME_TS_PCT_L10', 'AWAY_SCREEN_ASSISTS_L5', 'AWAY_PCT_PTS_3PT_L10', 'HOME_Q1_Q3_GAP_L10']},
        {"features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L10', 'AWAY_PCT_PTS_PAINT_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_OREB_PCT_L10', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'AWAY_MID_FREQ_L5', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_PCT_AST_FGM_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_PCT_PTS_3PT_L10', 'HOME_EFFICIENCY_TREND', 'AWAY_FTA_RATE_L10']},
        {"features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_EFFICIENCY_TREND', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'AWAY_FTA_RATE_S2D', 'HOME_PCT_PTS_PAINT_L5', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_MAX_UNANSWERED_RUN_L5', 'HOME_PCT_AST_FGM_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_CLUTCH_TOV_PCT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'AWAY_MAX_UNANSWERED_RUN_L3', 'HOME_TS_PCT_L10', 'AWAY_SCREEN_ASSISTS_L5', 'AWAY_PCT_PTS_3PT_L10', 'HOME_Q1_Q3_GAP_L10']},
        {"features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PIE_L5', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'AWAY_DEF_RATING_L5', 'HOME_AWAY_STREAK', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'HOME_NET_RATING_L10', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_CONTESTED_SHOTS_S2D', 'HOME_LOOSE_BALLS_RECOVERED_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_RUNS_10_0_COUNT_L10', 'HOME_PACE_L5', 'AWAY_DEF_RATING_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_S2D', 'AWAY_RIM_FREQ_L3', 'AWAY_MID_FREQ_L10', 'AWAY_PCT_PTS_3PT_L10', 'AWAY_AWAY_STREAK', 'AWAY_CHARGES_DRAWN_L3']},
        {"features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PIE_L5', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'AWAY_DEF_RATING_L5', 'HOME_PACE_L10', 'HOME_Q1_Q3_GAP_L3', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'HOME_MOREYBALL_INDEX_L5', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_CONTESTED_SHOTS_S2D', 'AWAY_DEF_RATING_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_MOREYBALL_INDEX_L3', 'AWAY_MID_FREQ_L10', 'AWAY_PCT_PTS_3PT_L10']},
        {"features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_L3', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L3', 'HOME_IS_B2B', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_AWAY_STREAK', 'AWAY_CLUTCH_TOV_PCT_L3', 'HOME_PACE_L10', 'HOME_MISSING_MIN_SUM', 'HOME_MISSING_USG_PCT_SUM_OPP', 'HOME_MISSING_PTS_SUM_OPP']},
        {"features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PIE_L5', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'AWAY_DEF_RATING_L5', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_CONTESTED_SHOTS_S2D', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_CLUTCH_TOV_PCT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_PCT_PTS_3PT_L10']},
        {"features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L10', 'AWAY_PCT_PTS_PAINT_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_OREB_PCT_L10', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'AWAY_MID_FREQ_L5', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_PCT_AST_FGM_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_PCT_PTS_3PT_L10', 'HOME_EFFICIENCY_TREND', 'AWAY_FTA_RATE_L10', 'HOME_MISSING_PIE_SUM', 'HOME_MISSING_DEF_RATING_SUM']},
        {"features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L10', 'AWAY_PCT_PTS_PAINT_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_OREB_PCT_L10', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'AWAY_MID_FREQ_L5', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_PCT_AST_FGM_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_PCT_PTS_3PT_L10', 'HOME_EFFICIENCY_TREND', 'AWAY_FTA_RATE_L10', 'HOME_MISSING_DEF_RATING_SUM', 'HOME_MISSING_PIE_SUM_OPP']},
        {"features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L10', 'AWAY_PCT_PTS_PAINT_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_OREB_PCT_L10', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'AWAY_MID_FREQ_L5', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_PCT_AST_FGM_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_PCT_PTS_3PT_L10', 'HOME_EFFICIENCY_TREND', 'AWAY_FTA_RATE_L10', 'HOME_MISSING_DEF_RATING_SUM']},
        {"features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_CONTESTED_SHOTS_L10', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'AWAY_SCREEN_ASSISTS_S2D', 'HOME_TM_TOV_PCT_S2D', 'AWAY_PCT_PTS_3PT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_PACE_L10', 'HOME_MISSING_PIE_SUM', 'HOME_MISSING_EFF_SUM']},
        {"features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_CONTESTED_SHOTS_L10', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'AWAY_SCREEN_ASSISTS_S2D', 'HOME_TM_TOV_PCT_S2D', 'AWAY_PCT_PTS_3PT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_PACE_L10', 'HOME_MISSING_MIN_SUM', 'HOME_MISSING_USG_PCT_SUM']},
        {"features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_CONTESTED_SHOTS_L10', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'AWAY_SCREEN_ASSISTS_S2D', 'HOME_TM_TOV_PCT_S2D', 'AWAY_PCT_PTS_3PT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_PACE_L10', 'HOME_MISSING_MIN_SUM', 'HOME_MISSING_EFF_SUM']},
        {"features": ['AWAY_EFFICIENCY_TREND', 'AWAY_OFF_RATING_L10_STD', 'HOME_PCT_PTS_3PT_L3', 'AWAY_CHARGES_DRAWN_L5', 'AWAY_DEF_RATING_L5', 'HOME_PACE_L10', 'HOME_Q1_Q3_GAP_L5', 'HOME_PCT_PTS_3PT_L10', 'HOME_Q1_Q3_GAP_S2D', 'HOME_CLUTCH_TS_PCT_L3', 'AWAY_MID_FREQ_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_TS_PCT_L10', 'AWAY_IS_B2B', 'HOME_RUNS_10_0_COUNT_S2D', 'AWAY_DEF_RATING_S2D', 'HOME_PCT_AST_FGM_S2D']},
        {"features": ['AWAY_CHARGES_DRAWN_L5', 'HOME_PCT_PTS_3PT_L3', 'AWAY_DEF_RATING_L5', 'HOME_Q1_Q3_GAP_L5', 'HOME_PACE_L10', 'HOME_Q1_Q3_GAP_S2D', 'AWAY_FTA_RATE_L10', 'AWAY_MID_FREQ_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_TS_PCT_L10', 'AWAY_IS_B2B', 'HOME_PCT_AST_FGM_S2D']},
        {"features": ['AWAY_PACE_L3', 'HOME_Q1_Q3_GAP_L10', 'AWAY_DEF_RATING_L3', 'HOME_PCT_PTS_3PT_L3', 'HOME_PCT_AST_FGM_L5', 'HOME_AWAY_STREAK', 'HOME_Q1_Q3_GAP_L5', 'HOME_PACE_L10', 'HOME_Q1_Q3_GAP_S2D', 'HOME_PACE_S2D', 'HOME_CLUTCH_TS_PCT_L3', 'AWAY_FTA_RATE_L10', 'AWAY_MID_FREQ_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_TS_PCT_L10', 'AWAY_IS_B2B', 'HOME_PCT_AST_FGM_S2D']},
        {"features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_L3', 'HOME_TS_PCT_L10', 'AWAY_DEF_RATING_L5', 'HOME_PCT_AST_FGM_L5', 'HOME_PCT_PTS_3PT_L3', 'HOME_NET_RATING_L10', 'AWAY_CHARGES_DRAWN_L5', 'AWAY_SCREEN_ASSISTS_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'HOME_PACE_L5', 'HOME_TM_TOV_PCT_S2D', 'HOME_CHARGES_DRAWN_L10', 'HOME_DEF_RATING_S2D', 'AWAY_CLUTCH_TOV_PCT_L3', 'HOME_PACE_L10', 'HOME_MISSING_PIE_SUM', 'HOME_MISSING_PTS_SUM', 'HOME_MISSING_DEF_RATING_SUM_OPP']},
        {"features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_L3', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L3', 'HOME_IS_B2B', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_AWAY_STREAK', 'AWAY_CLUTCH_TOV_PCT_L3', 'HOME_PACE_L10', 'HOME_MISSING_MIN_SUM', 'HOME_MISSING_USG_PCT_SUM_OPP', 'HOME_MISSING_PTS_SUM_OPP']},
        {"features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_CONTESTED_SHOTS_L10', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'AWAY_SCREEN_ASSISTS_S2D', 'HOME_TM_TOV_PCT_S2D', 'AWAY_PCT_PTS_3PT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_PACE_L10']},
        {"features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_PAINT_L5', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_PCT_AST_FGM_L10', 'HOME_MAX_UNANSWERED_RUN_L5', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'AWAY_PACE_S2D', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_TM_TOV_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_SCREEN_ASSISTS_L5', 'HOME_DEF_RATING_S2D', 'AWAY_PCT_PTS_3PT_L10', 'HOME_EFFICIENCY_TREND', 'HOME_MISSING_PIE_SUM', 'HOME_MISSING_USG_PCT_SUM_OPP']},
        {"features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_CONTESTED_SHOTS_L10', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'AWAY_SCREEN_ASSISTS_S2D', 'HOME_TM_TOV_PCT_S2D', 'AWAY_PCT_PTS_3PT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_PACE_L10', 'HOME_MISSING_EFF_SUM']},
        {"features": ['HOME_DEF_RATING_L5', 'HOME_Q1_Q3_GAP_L5', 'HOME_RUN_DEFICIT_RECOVERY_RATE_L5', 'HOME_REST_DAYS', 'HOME_Q1_Q3_GAP_L10', 'HOME_PACE_L10', 'HOME_Q1_Q3_GAP_S2D', 'HOME_CLUTCH_TS_PCT_L3', 'AWAY_MID_FREQ_L10', 'AWAY_IS_B2B', 'AWAY_CHARGES_DRAWN_L5', 'AWAY_Q1_Q3_GAP_L3', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_TS_PCT_L10', 'HOME_PCT_AST_FGM_S2D', 'AWAY_DEF_RATING_L3', 'HOME_PCT_PTS_3PT_L3', 'HOME_RUNS_10_0_COUNT_L3', 'AWAY_EFFICIENCY_TREND']}
    ]
    
    all_needed_features = set(['GAME_ID', 'GAME_DATE', 'SEASON_YEAR', 'HOME_TEAM', 'AWAY_TEAM', 'TW_SPREAD_SCORE', 'PLUS_MINUS'])
    for m in ALL_MODELS:
        all_needed_features.update(m['features'])
        
    cols_to_keep = [c for c in final_df.columns if c in all_needed_features]
    final_df = final_df[cols_to_keep]

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    final_df.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ 終極大表大功告成！已精簡並匯出至: {OUTPUT_CSV}")

if __name__ == "__main__":
    build_master_features()