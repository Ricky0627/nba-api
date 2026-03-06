import pandas as pd
import numpy as np
import os
import warnings

# 引入你的神級合併模組
from prepare_data import get_merged_dataframe

warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ 設定區
# ==========================================
OUTPUT_CSV = 'data/ml_features_master.csv'
INJURY_CSV = 'data/nba_advanced_injury_features.csv'

def load_and_merge_team_logs():
    print("📥 1. 載入並合併所有基礎與進階數據庫...")
    
    # 載入各個維度的數據表
    df_adv = get_merged_dataframe("boxscore_advanced")
    df_base = get_merged_dataframe("boxscore_base")
    df_score = get_merged_dataframe("boxscore_scoring")
    df_four = get_merged_dataframe("boxscore_four_factors")
    df_hustle = get_merged_dataframe("boxscore_hustle")
    
    # 載入 PBP 特徵
    df_clutch = get_merged_dataframe("team_features_clutch")
    df_shot = get_merged_dataframe("team_features_shot_profile")
    df_tov = get_merged_dataframe("team_features_turnover")
    df_mom = get_merged_dataframe("team_features_momentum")
    df_qtr = get_merged_dataframe("team_features_quarterly")

    # 🔥 關鍵修復：強制統一所有資料表的 GAME_ID 與 TEAM_ID 型態 (字串) 與大小寫
    all_dfs = [df_adv, df_base, df_score, df_four, df_hustle, df_clutch, df_shot, df_tov, df_mom, df_qtr]
    for df in all_dfs:
        if not df.empty:
            df.columns = [c.upper() for c in df.columns]
            if 'GAME_ID' in df.columns:
                df['GAME_ID'] = df['GAME_ID'].astype(str).str.zfill(10)
            if 'TEAM_ID' in df.columns:
                df['TEAM_ID'] = df['TEAM_ID'].astype(str)

    # 以 advanced 為基底
    base_cols = ['GAME_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'GAME_DATE', 'SEASON_YEAR', 'MATCHUP']
    
    # 防呆：確保所需的欄位存在
    avail_adv_cols = [c for c in ['PACE', 'DEF_RATING', 'NET_RATING', 'OFF_RATING', 'TS_PCT', 'EFG_PCT', 'TM_TOV_PCT', 'OREB_PCT', 'PIE'] if c in df_adv.columns]
    df_master = df_adv[base_cols + avail_adv_cols].copy()

    # 合併 Scoring
    score_cols = [c for c in ['PCT_PTS_3PT', 'PCT_PTS_PAINT', 'PCT_AST_FGM'] if c in df_score.columns]
    if score_cols:
        df_master = df_master.merge(df_score[['GAME_ID', 'TEAM_ID'] + score_cols], on=['GAME_ID', 'TEAM_ID'], how='left')

    # 合併 Four Factors
    if 'FTA_RATE' in df_four.columns:
        df_master = df_master.merge(df_four[['GAME_ID', 'TEAM_ID', 'FTA_RATE']], on=['GAME_ID', 'TEAM_ID'], how='left')

    # 合併 Hustle
    hustle_cols = [c for c in ['CONTESTED_SHOTS', 'LOOSE_BALLS_RECOVERED', 'CHARGES_DRAWN', 'SCREEN_ASSISTS'] if c in df_hustle.columns]
    if hustle_cols:
        df_master = df_master.merge(df_hustle[['GAME_ID', 'TEAM_ID'] + hustle_cols], on=['GAME_ID', 'TEAM_ID'], how='left')

    # 合併 PBP 特徵
    for pbp_df in [df_clutch, df_shot, df_tov, df_mom, df_qtr]:
        if pbp_df.empty: continue
        cols_to_use = [c for c in pbp_df.columns if c not in ['TEAM_ABBREVIATION', 'GAME_DATE', 'SEASON_YEAR', 'MATCHUP']] 
        df_master = df_master.merge(pbp_df[cols_to_use], on=['GAME_ID', 'TEAM_ID'], how='left')

    # 日期排序，這是計算滾動特徵的命脈
    df_master['GAME_DATE'] = pd.to_datetime(df_master['GAME_DATE'].str[:10])
    df_master = df_master.sort_values(['TEAM_ID', 'GAME_DATE']).reset_index(drop=True)
    
    # 填補 PBP 與 Hustle 可能產生的空值
    df_master = df_master.fillna(0)
    
    return df_master

def engineer_rolling_features(df):
    print("⏳ 2. 嚴格防洩漏：計算時間切片 (S2D, L10, L5, L3) 與衍生特徵...")
    
    target_metrics = [
        'PACE', 'DEF_RATING', 'NET_RATING', 'OFF_RATING', 'TS_PCT', 'EFG_PCT', 'TM_TOV_PCT', 'OREB_PCT', 'PIE',
        'PCT_PTS_3PT', 'PCT_PTS_PAINT', 'PCT_AST_FGM', 'FTA_RATE', 
        'CONTESTED_SHOTS', 'LOOSE_BALLS_RECOVERED', 'CHARGES_DRAWN', 'SCREEN_ASSISTS',
        'MID_FREQ', 'RIM_FREQ', 'MOREYBALL_INDEX', 
        'CLUTCH_TS_PCT', 'CLUTCH_TOV_PCT', 'RUNS_10_0_COUNT', 'MAX_UNANSWERED_RUN', 'RUN_DEFICIT_RECOVERY_RATE'
    ]
    
    df.columns = [c.upper() for c in df.columns]
    metrics = [c for c in target_metrics if c in df.columns]
    
    # 客製化特徵：Q1_Q3_Gap
    if 'Q1_PTS' in df.columns and 'Q3_PTS' in df.columns:
        df['Q1_Q3_GAP'] = df['Q1_PTS'] - df['Q3_PTS']
        metrics.append('Q1_Q3_GAP')
    
    # 建立分組，跨季不會互相干擾
    grouped = df.groupby(['SEASON_YEAR', 'TEAM_ID'])
    
    # === A. 基礎滾動平均 (Rolling Means: L3, L5, L10) ===
    windows = {'L3': 3, 'L5': 5, 'L10': 10}
    for w_name, w_size in windows.items():
        rolled = grouped[metrics].apply(lambda x: x.shift(1).rolling(w_size, min_periods=1).mean()).reset_index(level=[0,1], drop=True)
        rolled.columns = [f"{c}_{w_name}" for c in rolled.columns]
        df = df.join(rolled)
        
    # === B. 賽季至今平均 (Season-To-Date, S2D) ===
    s2d = grouped[metrics].apply(lambda x: x.shift(1).expanding(min_periods=1).mean()).reset_index(level=[0,1], drop=True)
    s2d.columns = [f"{c}_S2D" for c in s2d.columns]
    df = df.join(s2d)
    
    # === C. 特殊與降維指標 ===
    if 'OFF_RATING' in df.columns:
        df['OFF_RATING_L10_STD'] = grouped['OFF_RATING'].apply(lambda x: x.shift(1).rolling(10, min_periods=2).std()).reset_index(level=[0,1], drop=True)
    
    if 'NET_RATING_L5' in df.columns and 'NET_RATING_S2D' in df.columns:
        df['EFFICIENCY_TREND'] = df['NET_RATING_L5'] - df['NET_RATING_S2D']
    
    # 賽程疲勞度
    df['PREV_GAME_DATE'] = grouped['GAME_DATE'].shift(1)
    df['REST_DAYS'] = (df['GAME_DATE'] - df['PREV_GAME_DATE']).dt.days - 1
    df['REST_DAYS'] = df['REST_DAYS'].fillna(3).clip(upper=5) 
    df['IS_B2B'] = (df['REST_DAYS'] == 0).astype(int)
    
    # 客場連戰 (Away Streak)
    df['IS_AWAY'] = df['MATCHUP'].str.contains('@').astype(int)
    def calc_away_streak(s):
        streak = 0
        res = []
        for val in s:
            res.append(streak) 
            streak = streak + 1 if val == 1 else 0
        return pd.Series(res, index=s.index)
        
    df['AWAY_STREAK'] = grouped['IS_AWAY'].apply(calc_away_streak).reset_index(level=[0,1], drop=True)
    
    return df

def build_final_master_table(df_features):
    print("🧩 3. 將特徵合併至傷兵大表 (Base Table)...")
    
    if not os.path.exists(INJURY_CSV):
        print(f"❌ 嚴重錯誤：找不到 {INJURY_CSV}！請先確保 generate_injury.py 已執行。")
        return
        
    final_df = pd.read_csv(INJURY_CSV)
    final_df['game_id'] = final_df['game_id'].astype(str).str.zfill(10)
    
    df_features['GAME_ID'] = df_features['GAME_ID'].astype(str).str.zfill(10)
    df_features['TEAM_ID'] = df_features['TEAM_ID'].astype(str)
    
    # 抓取要 Join 的特徵欄位
    keep_cols = ['GAME_ID', 'TEAM_ID', 'REST_DAYS', 'IS_B2B', 'AWAY_STREAK', 'EFFICIENCY_TREND', 'OFF_RATING_L10_STD']
    keep_cols += [c for c in df_features.columns if c.endswith(('_L3', '_L5', '_L10', '_S2D'))]
    df_feat_clean = df_features[keep_cols].copy()
    
    # ==== 獲取球隊 ID 映射對照表 ====
    team_mapping = get_merged_dataframe("games")[['game_id', 'home_team_id', 'visitor_team_id']].drop_duplicates()
    team_mapping['game_id'] = team_mapping['game_id'].astype(str).str.zfill(10)
    team_mapping['home_team_id'] = team_mapping['home_team_id'].astype(str)
    team_mapping['visitor_team_id'] = team_mapping['visitor_team_id'].astype(str)
    
    final_df = final_df.merge(team_mapping, on='game_id', how='left')

    # ==== 對接主隊 (HOME) ====
    home_feats = df_feat_clean.copy()
    home_feats.columns = [f"HOME_{c}" if c not in ['GAME_ID', 'TEAM_ID'] else c for c in home_feats.columns]
    final_df = final_df.merge(home_feats, left_on=['game_id', 'home_team_id'], right_on=['GAME_ID', 'TEAM_ID'], how='left')
    final_df = final_df.drop(columns=['GAME_ID', 'TEAM_ID'])

    # ==== 對接客隊 (AWAY) ====
    away_feats = df_feat_clean.copy()
    away_feats.columns = [f"AWAY_{c}" if c not in ['GAME_ID', 'TEAM_ID'] else c for c in away_feats.columns]
    final_df = final_df.merge(away_feats, left_on=['game_id', 'visitor_team_id'], right_on=['GAME_ID', 'TEAM_ID'], how='left')
    final_df = final_df.drop(columns=['GAME_ID', 'TEAM_ID', 'home_team_id', 'visitor_team_id'])

    # 清除空值 (季初無滾動數據補0)
    final_df = final_df.fillna(0)
    
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    final_df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n✅ 終極大表大功告成！已輸出至: {OUTPUT_CSV}")

if __name__ == "__main__":
    print("🚀 啟動 NBA 終極特徵工程引擎 (Feature Store Builder)")
    df_logs = load_and_merge_team_logs()
    df_features = engineer_rolling_features(df_logs)
    build_final_master_table(df_features)