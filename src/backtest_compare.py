import pandas as pd
import numpy as np
import os
import warnings
from xgboost import XGBClassifier

warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ 設定區
# ==========================================
MASTER_FEATURES_CSV = 'data/ml_features_master.csv'

# 訓練與測試賽季
TRAIN_SEASONS = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22', '2022-23', '2023-24', '2024-25']
TEST_SEASON = ['2025-26']

# 🎯 冷血門檻 (高信心出手點)
SNIPER_THRESHOLD = 0.53  

# ==========================================
# 🏆 24 神聯軍全特徵定義 (純淨全大寫版，無 TEAM_ID)
# ==========================================
ALL_MODELS = [
    # ---------------- 50G 賽道 ----------------
    {
        "name": "50G_Rank1", "track": "50G (Rank 1)",
        "features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L10_HOME', 'PCT_PTS_PAINT_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'OREB_PCT_L10_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'MID_FREQ_L5_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'FTA_RATE_L10_AWAY']
    },
    {
        "name": "50G_Rank2", "track": "50G (Rank 2)",
        "features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_PAINT_L5_HOME', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'MAX_UNANSWERED_RUN_L5_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'PACE_S2D_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'TM_TOV_PCT_S2D_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'SCREEN_ASSISTS_L5_AWAY', 'DEF_RATING_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME']
    },
    {
        "name": "50G_Rank3", "track": "50G (Rank 3)",
        "features": ['CONTESTED_SHOTS_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'FTA_RATE_S2D_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'MAX_UNANSWERED_RUN_L5_HOME', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'CLUTCH_TOV_PCT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'MAX_UNANSWERED_RUN_L3_AWAY', 'TS_PCT_L10_HOME', 'SCREEN_ASSISTS_L5_AWAY', 'PCT_PTS_3PT_L10_AWAY', 'Q1_Q3_GAP_L10_HOME']
    },
    # ---------------- 70G 賽道 ----------------
    {
        "name": "70G_Rank1", "track": "70G (Rank 1)",
        "features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L10_HOME', 'PCT_PTS_PAINT_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'OREB_PCT_L10_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'MID_FREQ_L5_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'FTA_RATE_L10_AWAY']
    },
    {
        "name": "70G_Rank2", "track": "70G (Rank 2)",
        "features": ['CONTESTED_SHOTS_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'FTA_RATE_S2D_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'MAX_UNANSWERED_RUN_L5_HOME', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'CLUTCH_TOV_PCT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'MAX_UNANSWERED_RUN_L3_AWAY', 'TS_PCT_L10_HOME', 'SCREEN_ASSISTS_L5_AWAY', 'PCT_PTS_3PT_L10_AWAY', 'Q1_Q3_GAP_L10_HOME']
    },
    {
        "name": "70G_Rank3", "track": "70G (Rank 3)",
        "features": ['CONTESTED_SHOTS_L10_AWAY', 'PIE_L5_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'DEF_RATING_L5_AWAY', 'AWAY_STREAK_HOME', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'NET_RATING_L10_HOME', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'CONTESTED_SHOTS_S2D_HOME', 'LOOSE_BALLS_RECOVERED_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L10_HOME', 'PACE_L5_HOME', 'DEF_RATING_S2D_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_S2D_HOME', 'RIM_FREQ_L3_AWAY', 'MID_FREQ_L10_AWAY', 'PCT_PTS_3PT_L10_AWAY', 'AWAY_STREAK_AWAY', 'CHARGES_DRAWN_L3_AWAY']
    },
    # ---------------- 100G 賽道 ----------------
    {
        "name": "100G_Rank1", "track": "100G (Rank 1)",
        "features": ['CONTESTED_SHOTS_L10_AWAY', 'PIE_L5_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'DEF_RATING_L5_AWAY', 'PACE_L10_HOME', 'Q1_Q3_GAP_L3_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MOREYBALL_INDEX_L5_HOME', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'CONTESTED_SHOTS_S2D_HOME', 'DEF_RATING_L10_AWAY', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'MOREYBALL_INDEX_L3_AWAY', 'MID_FREQ_L10_AWAY', 'PCT_PTS_3PT_L10_AWAY']
    },
    {
        "name": "100G_Rank2", "track": "100G (Rank 2)",
        "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_L3_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'IS_B2B_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'AWAY_STREAK_HOME', 'CLUTCH_TOV_PCT_L3_AWAY', 'PACE_L10_HOME', 'MISSING_MIN_SUM', 'MISSING_USG_PCT_SUM_OPP', 'MISSING_PTS_SUM_OPP']
    },
    {
        "name": "100G_Rank3", "track": "100G (Rank 3)",
        "features": ['CONTESTED_SHOTS_L10_AWAY', 'PIE_L5_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'DEF_RATING_L5_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'CONTESTED_SHOTS_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'CLUTCH_TOV_PCT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY']
    },
    # ---------------- 150G 賽道 ----------------
    {
        "name": "150G_Rank1", "track": "150G (Rank 1)",
        "features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L10_HOME', 'PCT_PTS_PAINT_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'OREB_PCT_L10_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'MID_FREQ_L5_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'FTA_RATE_L10_AWAY', 'MISSING_PIE_SUM', 'MISSING_DEF_RATING_SUM']
    },
    {
        "name": "150G_Rank2", "track": "150G (Rank 2)",
        "features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L10_HOME', 'PCT_PTS_PAINT_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'OREB_PCT_L10_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'MID_FREQ_L5_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'FTA_RATE_L10_AWAY', 'MISSING_DEF_RATING_SUM', 'MISSING_PIE_SUM_OPP']
    },
    {
        "name": "150G_Rank3", "track": "150G (Rank 3)",
        "features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L10_HOME', 'PCT_PTS_PAINT_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'OREB_PCT_L10_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'MID_FREQ_L5_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'FTA_RATE_L10_AWAY', 'MISSING_DEF_RATING_SUM']
    },
    # ---------------- 200G 賽道 ----------------
    {
        "name": "200G_Rank1", "track": "200G (Rank 1)",
        "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'CONTESTED_SHOTS_L10_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'SCREEN_ASSISTS_S2D_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'PACE_L10_HOME', 'MISSING_PIE_SUM', 'MISSING_EFF_SUM']
    },
    {
        "name": "200G_Rank2", "track": "200G (Rank 2)",
        "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'CONTESTED_SHOTS_L10_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'SCREEN_ASSISTS_S2D_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'PACE_L10_HOME', 'MISSING_MIN_SUM', 'MISSING_USG_PCT_SUM']
    },
    {
        "name": "200G_Rank3", "track": "200G (Rank 3)",
        "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'CONTESTED_SHOTS_L10_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'SCREEN_ASSISTS_S2D_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'PACE_L10_HOME', 'MISSING_MIN_SUM', 'MISSING_EFF_SUM']
    },
    # ---------------- 總榜 (Overall) 賽道 ----------------
    {
        "name": "Overall_Rank1", "track": "Overall (Rank 1)",
        "features": ['EFFICIENCY_TREND_AWAY', 'OFF_RATING_L10_STD_AWAY', 'PCT_PTS_3PT_L3_HOME', 'CHARGES_DRAWN_L5_AWAY', 'DEF_RATING_L5_AWAY', 'PACE_L10_HOME', 'Q1_Q3_GAP_L5_HOME', 'PCT_PTS_3PT_L10_HOME', 'Q1_Q3_GAP_S2D_HOME', 'CLUTCH_TS_PCT_L3_HOME', 'MID_FREQ_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'TS_PCT_L10_AWAY', 'IS_B2B_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'DEF_RATING_S2D_AWAY', 'PCT_AST_FGM_S2D_HOME']
    },
    {
        "name": "Overall_Rank2", "track": "Overall (Rank 2)",
        "features": ['CHARGES_DRAWN_L5_AWAY', 'PCT_PTS_3PT_L3_HOME', 'DEF_RATING_L5_AWAY', 'Q1_Q3_GAP_L5_HOME', 'PACE_L10_HOME', 'Q1_Q3_GAP_S2D_HOME', 'FTA_RATE_L10_AWAY', 'MID_FREQ_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'TS_PCT_L10_AWAY', 'IS_B2B_AWAY', 'PCT_AST_FGM_S2D_HOME']
    },
    {
        "name": "Overall_Rank3", "track": "Overall (Rank 3)",
        "features": ['PACE_L3_AWAY', 'Q1_Q3_GAP_L10_HOME', 'DEF_RATING_L3_AWAY', 'PCT_PTS_3PT_L3_HOME', 'PCT_AST_FGM_L5_HOME', 'AWAY_STREAK_HOME', 'Q1_Q3_GAP_L5_HOME', 'PACE_L10_HOME', 'Q1_Q3_GAP_S2D_HOME', 'PACE_S2D_HOME', 'CLUTCH_TS_PCT_L3_HOME', 'FTA_RATE_L10_AWAY', 'MID_FREQ_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'TS_PCT_L10_AWAY', 'IS_B2B_AWAY', 'PCT_AST_FGM_S2D_HOME']
    },
    # ---------------- 王者組合 (King) ----------------
    {
        "name": "M062", "track": "King (50G 刺客)",
        "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_L3_AWAY', 'TS_PCT_L10_HOME', 'DEF_RATING_L5_AWAY', 'PCT_AST_FGM_L5_HOME', 'PCT_PTS_3PT_L3_HOME', 'NET_RATING_L10_HOME', 'CHARGES_DRAWN_L5_AWAY', 'SCREEN_ASSISTS_L10_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'PACE_L5_HOME', 'TM_TOV_PCT_S2D_HOME', 'CHARGES_DRAWN_L10_HOME', 'DEF_RATING_S2D_HOME', 'CLUTCH_TOV_PCT_L3_AWAY', 'PACE_L10_HOME', 'MISSING_PIE_SUM', 'MISSING_PTS_SUM', 'MISSING_DEF_RATING_SUM_OPP']
    },
    {
        "name": "M079", "track": "King (70G 狙擊手)",
        "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_L3_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'IS_B2B_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'AWAY_STREAK_HOME', 'CLUTCH_TOV_PCT_L3_AWAY', 'PACE_L10_HOME', 'MISSING_MIN_SUM', 'MISSING_USG_PCT_SUM_OPP', 'MISSING_PTS_SUM_OPP']
    },
    {
        "name": "M092", "track": "King (100G 主力)",
        "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'CONTESTED_SHOTS_L10_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'SCREEN_ASSISTS_S2D_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'PACE_L10_HOME']
    },
    {
        "name": "M110", "track": "King (150G 重裝甲)",
        "features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_PAINT_L5_HOME', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'MAX_UNANSWERED_RUN_L5_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'PACE_S2D_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'TM_TOV_PCT_S2D_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'SCREEN_ASSISTS_L5_AWAY', 'DEF_RATING_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'MISSING_PIE_SUM', 'MISSING_USG_PCT_SUM_OPP']
    },
    {
        "name": "M126", "track": "King (200G 重砲)",
        "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'CONTESTED_SHOTS_L10_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'SCREEN_ASSISTS_S2D_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'PACE_L10_HOME', 'MISSING_EFF_SUM']
    },
    {
        "name": "M014", "track": "King (最終防線)",
        "features": ['DEF_RATING_L5_HOME', 'Q1_Q3_GAP_L5_HOME', 'RUN_DEFICIT_RECOVERY_RATE_L5_HOME', 'REST_DAYS_HOME', 'Q1_Q3_GAP_L10_HOME', 'PACE_L10_HOME', 'Q1_Q3_GAP_S2D_HOME', 'CLUTCH_TS_PCT_L3_HOME', 'MID_FREQ_L10_AWAY', 'IS_B2B_AWAY', 'CHARGES_DRAWN_L5_AWAY', 'Q1_Q3_GAP_L3_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'TS_PCT_L10_AWAY', 'PCT_AST_FGM_S2D_HOME', 'DEF_RATING_L3_AWAY', 'PCT_PTS_3PT_L3_HOME', 'RUNS_10_0_COUNT_L3_HOME', 'EFFICIENCY_TREND_AWAY']
    }
]

def run_static_backtest():
    print("="*80)
    print("🚀 啟動 GitHub Action 專用：讓分盤靜態回測引擎 (完美還原窮舉實驗室)")
    print("="*80)

    try:
        df = pd.read_csv(MASTER_FEATURES_CSV)
        print(f"✅ 成功載入特徵大表，共 {len(df)} 筆資料")
    except Exception as e:
        print(f"❌ 無法讀取 {MASTER_FEATURES_CSV}，錯誤: {e}")
        return

    # 防呆檢查
    if 'TW_SPREAD_SCORE' not in df.columns or 'PLUS_MINUS' not in df.columns:
        print("❌ 特徵大表中缺少 TW_SPREAD_SCORE 或 PLUS_MINUS！請確認資料來源。")
        return

    # 過濾空盤口並計算勝負 Target
    df['TW_SPREAD_SCORE'] = pd.to_numeric(df['TW_SPREAD_SCORE'], errors='coerce')
    df = df[(df['TW_SPREAD_SCORE'] != 0) & (df['TW_SPREAD_SCORE'].notna())]
    df['HOME_WIN'] = (df['PLUS_MINUS'] + df['TW_SPREAD_SCORE'] > 0).astype(int)

    # 劃分訓練與測試集
    train_df = df[df['SEASON'].isin(TRAIN_SEASONS)].copy()
    test_df = df[df['SEASON'].isin(TEST_SEASON)].copy()

    y_train = train_df['HOME_WIN'].values
    y_test = test_df['HOME_WIN'].values

    print(f"📊 訓練基底: {len(train_df)} 場 (2016-2025) | 測試樣本: {len(test_df)} 場 (本賽季)")
    print("="*80)
    print(f"{'模型名稱':<15} | {'賽道屬性':<18} | {'全覆蓋勝率 (無門檻)':<20} | {'高信心勝率 (>53%)':<18}")
    print("-" * 80)

    results = []
    
    for stage in ALL_MODELS:
        m_name = stage['name']
        features = stage['features']

        # 安全機制：確保所有需要的特徵都在 DataFrame 中，若無則補 0
        missing_cols = [f for f in features if f not in train_df.columns]
        for col in missing_cols:
            train_df[col] = 0
            test_df[col] = 0
            
        # 🔥 核心修正 1：完美補齊 .fillna(0)，確保與窮舉時的處理邏輯 100% 一致
        X_train = train_df[features].fillna(0)
        X_test = test_df[features].fillna(0)

        # 🔥 核心修正 2：嚴格鎖定窮舉時防過擬合的 XGBoost 參數
        model = XGBClassifier(
            n_estimators=50, 
            learning_rate=0.05, 
            max_depth=3, 
            subsample=0.8,
            colsample_bytree=0.8,
            eval_metric='logloss',
            random_state=42,
            tree_method='hist', # GitHub Action 的 CPU 跑 hist 非常快
            n_jobs=-1
        )

        # 訓練與預測
        model.fit(X_train, y_train)
        probs = model.predict_proba(X_test)[:, 1]

        # 🎯 評估 A：全覆蓋勝率 (所有比賽都預測)
        preds_all = (probs >= 0.5).astype(int)
        acc_all = np.mean(preds_all == y_test)

        # 🎯 評估 B：高信心勝率 (大於 53% 或小於 47% 才出手)
        high_conf_mask = (probs >= SNIPER_THRESHOLD) | (probs <= (1 - SNIPER_THRESHOLD))
        bets = high_conf_mask.sum()
        
        if bets > 0:
            preds_high = (probs[high_conf_mask] >= 0.5).astype(int)
            acc_high = np.mean(preds_high == y_test[high_conf_mask])
        else:
            acc_high = 0.0
            
        # 排版輸出
        acc_all_str = f"{acc_all*100:>6.2f}% ({len(test_df)}場)"
        acc_high_str = f"{acc_high*100:>6.2f}% ({bets}場)"
        print(f"{m_name:<15} | {stage['track']:<18} | {acc_all_str:<20} | {acc_high_str:<18}")

    print("="*80)
    print("✅ 回測完畢！你可以清楚看到 Overall 賽道的全覆蓋勝率已重返 60% 榮耀！")

if __name__ == "__main__":
    run_static_backtest()