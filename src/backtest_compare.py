import pandas as pd
import numpy as np
import os
import warnings
from datetime import datetime
from xgboost import XGBClassifier
from prepare_data import get_merged_dataframe

warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ 設定區
# ==========================================
MASTER_FEATURES_CSV = 'data/ml_features_master.csv'

# 訓練與測試賽季
TRAIN_SEASONS = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22', '2022-23', '2023-24', '2024-25']
TEST_SEASON = ['2025-26']

# 🎯 冷血門檻
SNIPER_THRESHOLD = 0.53  

# ==========================================
# 🏆 24 神聯軍全特徵定義 (全大寫對齊)
# ==========================================
ALL_MODELS = [
    # ---------------- 50G 賽道 ----------------
    {"name": "50G_Rank1", "track": "50G (Rank 1)", "features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L10_HOME', 'PCT_PTS_PAINT_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'OREB_PCT_L10_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'MID_FREQ_L5_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'FTA_RATE_L10_AWAY']},
    {"name": "50G_Rank2", "track": "50G (Rank 2)", "features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_PAINT_L5_HOME', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'MAX_UNANSWERED_RUN_L5_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'PACE_S2D_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'TM_TOV_PCT_S2D_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'SCREEN_ASSISTS_L5_AWAY', 'DEF_RATING_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME']},
    {"name": "50G_Rank3", "track": "50G (Rank 3)", "features": ['CONTESTED_SHOTS_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'FTA_RATE_S2D_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'MAX_UNANSWERED_RUN_L5_HOME', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'CLUTCH_TOV_PCT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'MAX_UNANSWERED_RUN_L3_AWAY', 'TS_PCT_L10_HOME', 'SCREEN_ASSISTS_L5_AWAY', 'PCT_PTS_3PT_L10_AWAY', 'Q1_Q3_GAP_L10_HOME']},
    # ---------------- 70G 賽道 ----------------
    {"name": "70G_Rank1", "track": "70G (Rank 1)", "features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L10_HOME', 'PCT_PTS_PAINT_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'OREB_PCT_L10_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'MID_FREQ_L5_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'FTA_RATE_L10_AWAY']},
    {"name": "70G_Rank2", "track": "70G (Rank 2)", "features": ['CONTESTED_SHOTS_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'FTA_RATE_S2D_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'MAX_UNANSWERED_RUN_L5_HOME', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'CLUTCH_TOV_PCT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'MAX_UNANSWERED_RUN_L3_AWAY', 'TS_PCT_L10_HOME', 'SCREEN_ASSISTS_L5_AWAY', 'PCT_PTS_3PT_L10_AWAY', 'Q1_Q3_GAP_L10_HOME']},
    {"name": "70G_Rank3", "track": "70G (Rank 3)", "features": ['CONTESTED_SHOTS_L10_AWAY', 'PIE_L5_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'DEF_RATING_L5_AWAY', 'AWAY_STREAK_HOME', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'NET_RATING_L10_HOME', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'CONTESTED_SHOTS_S2D_HOME', 'LOOSE_BALLS_RECOVERED_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L10_HOME', 'PACE_L5_HOME', 'DEF_RATING_S2D_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_S2D_HOME', 'RIM_FREQ_L3_AWAY', 'MID_FREQ_L10_AWAY', 'PCT_PTS_3PT_L10_AWAY', 'AWAY_STREAK_AWAY', 'CHARGES_DRAWN_L3_AWAY']},
    # ---------------- 100G 賽道 ----------------
    {"name": "100G_Rank1", "track": "100G (Rank 1)", "features": ['CONTESTED_SHOTS_L10_AWAY', 'PIE_L5_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'DEF_RATING_L5_AWAY', 'PACE_L10_HOME', 'Q1_Q3_GAP_L3_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MOREYBALL_INDEX_L5_HOME', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'CONTESTED_SHOTS_S2D_HOME', 'DEF_RATING_L10_AWAY', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'MOREYBALL_INDEX_L3_AWAY', 'MID_FREQ_L10_AWAY', 'PCT_PTS_3PT_L10_AWAY']},
    {"name": "100G_Rank2", "track": "100G (Rank 2)", "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_L3_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'IS_B2B_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'AWAY_STREAK_HOME', 'CLUTCH_TOV_PCT_L3_AWAY', 'PACE_L10_HOME', 'MISSING_MIN_SUM', 'MISSING_USG_PCT_SUM_OPP', 'MISSING_PTS_SUM_OPP']},
    {"name": "100G_Rank3", "track": "100G (Rank 3)", "features": ['CONTESTED_SHOTS_L10_AWAY', 'PIE_L5_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'DEF_RATING_L5_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'CONTESTED_SHOTS_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'CLUTCH_TOV_PCT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY']},
    # ---------------- 150G 賽道 ----------------
    {"name": "150G_Rank1", "track": "150G (Rank 1)", "features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L10_HOME', 'PCT_PTS_PAINT_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'OREB_PCT_L10_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'MID_FREQ_L5_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'FTA_RATE_L10_AWAY', 'MISSING_PIE_SUM', 'MISSING_DEF_RATING_SUM']},
    {"name": "150G_Rank2", "track": "150G (Rank 2)", "features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L10_HOME', 'PCT_PTS_PAINT_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'OREB_PCT_L10_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'MID_FREQ_L5_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'FTA_RATE_L10_AWAY', 'MISSING_DEF_RATING_SUM', 'MISSING_PIE_SUM_OPP']},
    {"name": "150G_Rank3", "track": "150G (Rank 3)", "features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L10_HOME', 'PCT_PTS_PAINT_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'OREB_PCT_L10_AWAY', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'MID_FREQ_L5_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'FTA_RATE_L10_AWAY', 'MISSING_DEF_RATING_SUM']},
    # ---------------- 200G 賽道 ----------------
    {"name": "200G_Rank1", "track": "200G (Rank 1)", "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'CONTESTED_SHOTS_L10_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'SCREEN_ASSISTS_S2D_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'PACE_L10_HOME', 'MISSING_PIE_SUM', 'MISSING_EFF_SUM']},
    {"name": "200G_Rank2", "track": "200G (Rank 2)", "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'CONTESTED_SHOTS_L10_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'SCREEN_ASSISTS_S2D_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'PACE_L10_HOME', 'MISSING_MIN_SUM', 'MISSING_USG_PCT_SUM']},
    {"name": "200G_Rank3", "track": "200G (Rank 3)", "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'CONTESTED_SHOTS_L10_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'SCREEN_ASSISTS_S2D_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'PACE_L10_HOME', 'MISSING_MIN_SUM', 'MISSING_EFF_SUM']},
    # ---------------- 總排名 (Overall) ----------------
    {"name": "Overall_Rank1", "track": "Overall (Rank 1)", "features": ['EFFICIENCY_TREND_AWAY', 'OFF_RATING_L10_STD_AWAY', 'PCT_PTS_3PT_L3_HOME', 'CHARGES_DRAWN_L5_AWAY', 'DEF_RATING_L5_AWAY', 'PACE_L10_HOME', 'Q1_Q3_GAP_L5_HOME', 'PCT_PTS_3PT_L10_HOME', 'Q1_Q3_GAP_S2D_HOME', 'CLUTCH_TS_PCT_L3_HOME', 'MID_FREQ_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'TS_PCT_L10_AWAY', 'IS_B2B_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'DEF_RATING_S2D_AWAY', 'PCT_AST_FGM_S2D_HOME']},
    {"name": "Overall_Rank2", "track": "Overall (Rank 2)", "features": ['CHARGES_DRAWN_L5_AWAY', 'PCT_PTS_3PT_L3_HOME', 'DEF_RATING_L5_AWAY', 'Q1_Q3_GAP_L5_HOME', 'PACE_L10_HOME', 'Q1_Q3_GAP_S2D_HOME', 'FTA_RATE_L10_AWAY', 'MID_FREQ_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'TS_PCT_L10_AWAY', 'IS_B2B_AWAY', 'PCT_AST_FGM_S2D_HOME']},
    {"name": "Overall_Rank3", "track": "Overall (Rank 3)", "features": ['PACE_L3_AWAY', 'Q1_Q3_GAP_L10_HOME', 'DEF_RATING_L3_AWAY', 'PCT_PTS_3PT_L3_HOME', 'PCT_AST_FGM_L5_HOME', 'AWAY_STREAK_HOME', 'Q1_Q3_GAP_L5_HOME', 'PACE_L10_HOME', 'Q1_Q3_GAP_S2D_HOME', 'PACE_S2D_HOME', 'CLUTCH_TS_PCT_L3_HOME', 'FTA_RATE_L10_AWAY', 'MID_FREQ_L10_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'TS_PCT_L10_AWAY', 'IS_B2B_AWAY', 'PCT_AST_FGM_S2D_HOME']},
    # ---------------- 絕對王者瀑布流 (6 Kings) ----------------
    {"name": "M062", "track": "King (50G 刺客)", "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_L3_AWAY', 'TS_PCT_L10_HOME', 'DEF_RATING_L5_AWAY', 'PCT_AST_FGM_L5_HOME', 'PCT_PTS_3PT_L3_HOME', 'NET_RATING_L10_HOME', 'CHARGES_DRAWN_L5_AWAY', 'SCREEN_ASSISTS_L10_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'PACE_L5_HOME', 'TM_TOV_PCT_S2D_HOME', 'CHARGES_DRAWN_L10_HOME', 'DEF_RATING_S2D_HOME', 'CLUTCH_TOV_PCT_L3_AWAY', 'PACE_L10_HOME', 'MISSING_PIE_SUM', 'MISSING_PTS_SUM', 'MISSING_DEF_RATING_SUM_OPP']},
    {"name": "M079", "track": "King (70G 狙擊手)", "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_L3_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'IS_B2B_HOME', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'AWAY_STREAK_HOME', 'CLUTCH_TOV_PCT_L3_AWAY', 'PACE_L10_HOME', 'MISSING_MIN_SUM', 'MISSING_USG_PCT_SUM_OPP', 'MISSING_PTS_SUM_OPP']},
    {"name": "M092", "track": "King (100G 主力)", "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'CONTESTED_SHOTS_L10_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'SCREEN_ASSISTS_S2D_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'PACE_L10_HOME']},
    {"name": "M110", "track": "King (150G 重裝甲)", "features": ['CONTESTED_SHOTS_L10_AWAY', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_PAINT_L5_HOME', 'PACE_L10_HOME', 'LOOSE_BALLS_RECOVERED_S2D_HOME', 'MID_FREQ_S2D_AWAY', 'FTA_RATE_L3_AWAY', 'EFG_PCT_L10_HOME', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L5_HOME', 'MOREYBALL_INDEX_L10_HOME', 'CHARGES_DRAWN_L10_AWAY', 'PCT_AST_FGM_L10_HOME', 'MAX_UNANSWERED_RUN_L5_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'DEF_RATING_L10_HOME', 'PACE_S2D_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'TM_TOV_PCT_S2D_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'TS_PCT_L10_HOME', 'SCREEN_ASSISTS_L5_AWAY', 'DEF_RATING_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'EFFICIENCY_TREND_HOME', 'MISSING_PIE_SUM', 'MISSING_USG_PCT_SUM_OPP']},
    {"name": "M126", "track": "King (200G 重砲)", "features": ['LOOSE_BALLS_RECOVERED_S2D_HOME', 'CONTESTED_SHOTS_L10_AWAY', 'TS_PCT_L10_HOME', 'PCT_AST_FGM_L5_HOME', 'PACE_S2D_HOME', 'PCT_PTS_3PT_L3_HOME', 'PCT_PTS_PAINT_L5_AWAY', 'CLUTCH_TS_PCT_S2D_HOME', 'EFG_PCT_L10_HOME', 'SCREEN_ASSISTS_S2D_AWAY', 'TM_TOV_PCT_S2D_HOME', 'PCT_PTS_3PT_L10_AWAY', 'RUNS_10_0_COUNT_L3_AWAY', 'RUNS_10_0_COUNT_S2D_HOME', 'PACE_L10_HOME', 'MISSING_EFF_SUM']},
    {"name": "M014", "track": "King (最終防線)", "features": ['DEF_RATING_L5_HOME', 'Q1_Q3_GAP_L5_HOME', 'RUN_DEFICIT_RECOVERY_RATE_L5_HOME', 'REST_DAYS_HOME', 'Q1_Q3_GAP_L10_HOME', 'PACE_L10_HOME', 'Q1_Q3_GAP_S2D_HOME', 'CLUTCH_TS_PCT_L3_HOME', 'MID_FREQ_L10_AWAY', 'IS_B2B_AWAY', 'CHARGES_DRAWN_L5_AWAY', 'Q1_Q3_GAP_L3_AWAY', 'PCT_PTS_PAINT_L5_HOME', 'TS_PCT_L10_AWAY', 'PCT_AST_FGM_S2D_HOME', 'DEF_RATING_L3_AWAY', 'PCT_PTS_3PT_L3_HOME', 'RUNS_10_0_COUNT_L3_HOME', 'EFFICIENCY_TREND_AWAY']}
]

# 🚨 防過擬合設定 (50棵樹, 深度3)
def get_xgb_model():
    return XGBClassifier(
            n_estimators=50,       # 必須是 50
            learning_rate=0.05,
            max_depth=3,           # 必須是 3
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            eval_metric='logloss'
        # 移除 learning_rate，讓模型大膽推測機率！
    )

def load_data_for_backtest():
    print("📥 正在載入已包含賠率的特徵大表...")
    df_master = pd.read_csv(MASTER_FEATURES_CSV, low_memory=False)
    df_master.columns = [c.upper() for c in df_master.columns]
    df_master['GAME_ID'] = df_master['GAME_ID'].astype(str).str.zfill(10)
    
    df = df_master.copy()
    
    if 'GAME_DATE' not in df.columns or 'SEASON_YEAR' not in df.columns:
        df_base = get_merged_dataframe("boxscore_base")
        df_base.columns = [c.upper() for c in df_base.columns]
        df_dates = df_base[['GAME_ID', 'GAME_DATE', 'SEASON_YEAR']].drop_duplicates()
        df_dates['GAME_ID'] = df_dates['GAME_ID'].astype(str).str.zfill(10)
        if 'GAME_DATE' in df.columns: df = df.drop(columns=['GAME_DATE'])
        if 'SEASON_YEAR' in df.columns: df = df.drop(columns=['SEASON_YEAR'])
        df = df.merge(df_dates, on='GAME_ID', how='left')
        
    df['TW_SPREAD_SCORE'] = pd.to_numeric(df['TW_SPREAD_SCORE'], errors='coerce')
    df = df[(df['TW_SPREAD_SCORE'] != 0) & (df['TW_SPREAD_SCORE'].notna())]
    df = df.dropna(subset=['PLUS_MINUS'])
    
    df['HOME_WIN'] = (df['PLUS_MINUS'] + df['TW_SPREAD_SCORE'] > 0).astype(int)
    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'].str[:10]) 
    df = df.dropna(subset=['GAME_DATE', 'SEASON_YEAR'])
    
    # 🚨 關鍵防呆：自動補齊可能缺失的特徵，防止 KeyError
    print("   🔧 正在為歷史資料補齊缺少的動態特徵 (如傷病等)...")
    all_needed_features = set()
    for m in ALL_MODELS:
        all_needed_features.update(m['features'])
    for col in all_needed_features:
        if col not in df.columns:
            df[col] = 0
            
    df = df[df['SEASON_YEAR'].isin(TRAIN_SEASONS + TEST_SEASON)]
    df = df.sort_values('GAME_DATE').reset_index(drop=True)
    return df

def run_arena():
    df = load_data_for_backtest()
    
    history_df = df[df['SEASON_YEAR'].isin(TRAIN_SEASONS)].copy()
    test_df = df[df['SEASON_YEAR'].isin(TEST_SEASON)].copy()
    
    if history_df.empty or test_df.empty: return
        
    test_dates = sorted(test_df['GAME_DATE'].unique())
    print(f"\n⚔️  24 神聯軍：防過擬合參數對齊版 (過濾 >= {SNIPER_THRESHOLD*100}% 絕對信心)")
    print(f"📊 訓練基底: {len(history_df)} 場賽事 ({', '.join(TRAIN_SEASONS)})")
    print(f"📅 本季測試: {len(test_df)} 場賽事\n")
    print("="*100)

    results = []

    for i, model_config in enumerate(ALL_MODELS):
        m_name = model_config['name']
        features = model_config['features']
        
        threshold = 0.50 if "Overall" in m_name else SNIPER_THRESHOLD
        
        static_model = get_xgb_model()
        X_train_static = history_df[features].fillna(0)
        static_model.fit(X_train_static, history_df['HOME_WIN'])
        
        static_correct, static_bets = 0, 0
        rolling_correct, rolling_bets = 0, 0

        for current_date in test_dates:
            day_test = df[df['GAME_DATE'] == current_date]
            X_test = day_test[features].fillna(0)
            y_test = day_test['HOME_WIN'].values
            
            # --- 靜態預測 ---
            s_probs = static_model.predict_proba(X_test)
            s_max_probs = np.max(s_probs, axis=1)
            s_preds = np.argmax(s_probs, axis=1)
            
            s_bet_mask = s_max_probs >= threshold
            static_correct += np.sum((s_preds == y_test)[s_bet_mask])
            static_bets += np.sum(s_bet_mask)
            
            # --- 滾動訓練與預測 ---
            rolling_train_df = df[df['GAME_DATE'] < current_date]
            rolling_model = get_xgb_model()
            X_train_rolling = rolling_train_df[features].fillna(0)
            rolling_model.fit(X_train_rolling, rolling_train_df['HOME_WIN'])
            
            r_probs = rolling_model.predict_proba(X_test)
            r_max_probs = np.max(r_probs, axis=1)
            r_preds = np.argmax(r_probs, axis=1)
            
            r_bet_mask = r_max_probs >= threshold
            rolling_correct += np.sum((r_preds == y_test)[r_bet_mask])
            rolling_bets += np.sum(r_bet_mask)

        static_acc = static_correct / static_bets if static_bets > 0 else 0
        rolling_acc = rolling_correct / rolling_bets if rolling_bets > 0 else 0
        
        if rolling_acc > static_acc: winner, gap = "滾動", rolling_acc - static_acc
        elif static_acc > rolling_acc: winner, gap = "靜態", static_acc - rolling_acc
        else: winner, gap = "平手", 0
            
        results.append({
            "Model": m_name,
            "Track": model_config['track'],
            "Static_Bets": static_bets,
            "Static_Acc": round(static_acc * 100, 2),
            "Rolling_Bets": rolling_bets,
            "Rolling_Acc": round(rolling_acc * 100, 2),
            "Winner": winner,
            "Gap": round(gap * 100, 2)
        })

    print("\n" + "="*100)
    print(f"{'🏅 24 神聯軍：參數對齊 最終戰果表':^90}")
    print("="*100)
    
    res_df = pd.DataFrame(results)
    print(f"{'模型名稱':<15} | {'賽道':<18} | {'靜態勝率 (下注數)':<16} | {'滾動勝率 (下注數)':<16} | {'最終贏家':<4}")
    print("-" * 100)
    
    for _, row in res_df.iterrows():
        winner_str = f"🏆 {row['Winner']}" if row['Gap'] > 0 else "🤝 平手"
        gap_str = f"(+{row['Gap']}%)" if row['Gap'] > 0 else ""
        
        static_str = f"{row['Static_Acc']:>5}% ({row['Static_Bets']}場)"
        rolling_str = f"{row['Rolling_Acc']:>5}% ({row['Rolling_Bets']}場)"
        
        print(f"{row['Model']:<17} | {row['Track']:<20} | {static_str:<18} | {rolling_str:<18} | {winner_str} {gap_str}")
        
    print("="*100)

if __name__ == "__main__":
    run_arena()