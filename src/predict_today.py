import pandas as pd
import numpy as np
import os
import warnings
from datetime import datetime
from xgboost import XGBClassifier

warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ 設定與路徑區
# ==========================================
UPCOMING_CSV = 'data/upcoming_games.csv'
MASTER_FEATURES_CSV = 'data/ml_features_master.csv'
MODEL_DIR = 'models/'  
OUTPUT_PREDICTION = 'data/predictions_history_log.csv' 

# ==========================================
# 🏆 24 神聯軍全特徵定義 (封印權重專用版)
# ==========================================
ALL_MODELS = [
    # ---------------- 50G 賽道 ----------------
    {
        "name": "50G_Rank1", "track": "50G (Rank 1)",
        "features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L10', 'AWAY_PCT_PTS_PAINT_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_OREB_PCT_L10', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'AWAY_MID_FREQ_L5', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_PCT_AST_FGM_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_PCT_PTS_3PT_L10', 'HOME_EFFICIENCY_TREND', 'AWAY_FTA_RATE_L10']
    },
    {
        "name": "50G_Rank2", "track": "50G (Rank 2)",
        "features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_PAINT_L5', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_PCT_AST_FGM_L10', 'HOME_MAX_UNANSWERED_RUN_L5', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'AWAY_PACE_S2D', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_TM_TOV_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_SCREEN_ASSISTS_L5', 'HOME_DEF_RATING_S2D', 'AWAY_PCT_PTS_3PT_L10', 'HOME_EFFICIENCY_TREND']
    },
    {
        "name": "50G_Rank3", "track": "50G (Rank 3)",
        "features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_EFFICIENCY_TREND', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'AWAY_FTA_RATE_S2D', 'HOME_PCT_PTS_PAINT_L5', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_MAX_UNANSWERED_RUN_L5', 'HOME_PCT_AST_FGM_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_CLUTCH_TOV_PCT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'AWAY_MAX_UNANSWERED_RUN_L3', 'HOME_TS_PCT_L10', 'AWAY_SCREEN_ASSISTS_L5', 'AWAY_PCT_PTS_3PT_L10', 'HOME_Q1_Q3_GAP_L10']
    },
    # ---------------- 70G 賽道 ----------------
    {
        "name": "70G_Rank1", "track": "70G (Rank 1)",
        "features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L10', 'AWAY_PCT_PTS_PAINT_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_OREB_PCT_L10', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'AWAY_MID_FREQ_L5', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_PCT_AST_FGM_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_PCT_PTS_3PT_L10', 'HOME_EFFICIENCY_TREND', 'AWAY_FTA_RATE_L10']
    },
    {
        "name": "70G_Rank2", "track": "70G (Rank 2)",
        "features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_EFFICIENCY_TREND', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'AWAY_FTA_RATE_S2D', 'HOME_PCT_PTS_PAINT_L5', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_MAX_UNANSWERED_RUN_L5', 'HOME_PCT_AST_FGM_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_CLUTCH_TOV_PCT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'AWAY_MAX_UNANSWERED_RUN_L3', 'HOME_TS_PCT_L10', 'AWAY_SCREEN_ASSISTS_L5', 'AWAY_PCT_PTS_3PT_L10', 'HOME_Q1_Q3_GAP_L10']
    },
    {
        "name": "70G_Rank3", "track": "70G (Rank 3)",
        "features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PIE_L5', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'AWAY_DEF_RATING_L5', 'HOME_AWAY_STREAK', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'HOME_NET_RATING_L10', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_CONTESTED_SHOTS_S2D', 'HOME_LOOSE_BALLS_RECOVERED_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_RUNS_10_0_COUNT_L10', 'HOME_PACE_L5', 'AWAY_DEF_RATING_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_S2D', 'AWAY_RIM_FREQ_L3', 'AWAY_MID_FREQ_L10', 'AWAY_PCT_PTS_3PT_L10', 'AWAY_AWAY_STREAK', 'AWAY_CHARGES_DRAWN_L3']
    },
    # ---------------- 100G 賽道 ----------------
    {
        "name": "100G_Rank1", "track": "100G (Rank 1)",
        "features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PIE_L5', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'AWAY_DEF_RATING_L5', 'HOME_PACE_L10', 'HOME_Q1_Q3_GAP_L3', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'HOME_MOREYBALL_INDEX_L5', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_CONTESTED_SHOTS_S2D', 'AWAY_DEF_RATING_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_MOREYBALL_INDEX_L3', 'AWAY_MID_FREQ_L10', 'AWAY_PCT_PTS_3PT_L10']
    },
    {
        "name": "100G_Rank2", "track": "100G (Rank 2)",
        "features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_L3', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L3', 'HOME_IS_B2B', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_AWAY_STREAK', 'AWAY_CLUTCH_TOV_PCT_L3', 'HOME_PACE_L10', 'HOME_MISSING_MIN_SUM', 'HOME_MISSING_USG_PCT_SUM_OPP', 'HOME_MISSING_PTS_SUM_OPP']
    },
    {
        "name": "100G_Rank3", "track": "100G (Rank 3)",
        "features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PIE_L5', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'AWAY_DEF_RATING_L5', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_CONTESTED_SHOTS_S2D', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_CLUTCH_TOV_PCT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_PCT_PTS_3PT_L10']
    },
    # ---------------- 150G 賽道 ----------------
    {
        "name": "150G_Rank1", "track": "150G (Rank 1)",
        "features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L10', 'AWAY_PCT_PTS_PAINT_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_OREB_PCT_L10', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'AWAY_MID_FREQ_L5', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_PCT_AST_FGM_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_PCT_PTS_3PT_L10', 'HOME_EFFICIENCY_TREND', 'AWAY_FTA_RATE_L10', 'HOME_MISSING_PIE_SUM', 'HOME_MISSING_DEF_RATING_SUM']
    },
    {
        "name": "150G_Rank2", "track": "150G (Rank 2)",
        "features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L10', 'AWAY_PCT_PTS_PAINT_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_OREB_PCT_L10', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'AWAY_MID_FREQ_L5', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_PCT_AST_FGM_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_PCT_PTS_3PT_L10', 'HOME_EFFICIENCY_TREND', 'AWAY_FTA_RATE_L10', 'HOME_MISSING_DEF_RATING_SUM', 'HOME_MISSING_PIE_SUM_OPP']
    },
    {
        "name": "150G_Rank3", "track": "150G (Rank 3)",
        "features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L10', 'AWAY_PCT_PTS_PAINT_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_OREB_PCT_L10', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'AWAY_MID_FREQ_L5', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_PCT_AST_FGM_L10', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_PCT_PTS_3PT_L10', 'HOME_EFFICIENCY_TREND', 'AWAY_FTA_RATE_L10', 'HOME_MISSING_DEF_RATING_SUM']
    },
    # ---------------- 200G 賽道 ----------------
    {
        "name": "200G_Rank1", "track": "200G (Rank 1)",
        "features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_CONTESTED_SHOTS_L10', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'AWAY_SCREEN_ASSISTS_S2D', 'HOME_TM_TOV_PCT_S2D', 'AWAY_PCT_PTS_3PT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_PACE_L10', 'HOME_MISSING_PIE_SUM', 'HOME_MISSING_EFF_SUM']
    },
    {
        "name": "200G_Rank2", "track": "200G (Rank 2)",
        "features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_CONTESTED_SHOTS_L10', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'AWAY_SCREEN_ASSISTS_S2D', 'HOME_TM_TOV_PCT_S2D', 'AWAY_PCT_PTS_3PT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_PACE_L10', 'HOME_MISSING_MIN_SUM', 'HOME_MISSING_USG_PCT_SUM']
    },
    {
        "name": "200G_Rank3", "track": "200G (Rank 3)",
        "features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_CONTESTED_SHOTS_L10', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'AWAY_SCREEN_ASSISTS_S2D', 'HOME_TM_TOV_PCT_S2D', 'AWAY_PCT_PTS_3PT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_PACE_L10', 'HOME_MISSING_MIN_SUM', 'HOME_MISSING_EFF_SUM']
    },
    # ---------------- 總排名 (Overall) ----------------
    {
        "name": "Overall_Rank1", "track": "Overall (Rank 1)",
        "features": ['AWAY_EFFICIENCY_TREND', 'AWAY_OFF_RATING_L10_STD', 'HOME_PCT_PTS_3PT_L3', 'AWAY_CHARGES_DRAWN_L5', 'AWAY_DEF_RATING_L5', 'HOME_PACE_L10', 'HOME_Q1_Q3_GAP_L5', 'HOME_PCT_PTS_3PT_L10', 'HOME_Q1_Q3_GAP_S2D', 'HOME_CLUTCH_TS_PCT_L3', 'AWAY_MID_FREQ_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_TS_PCT_L10', 'AWAY_IS_B2B', 'HOME_RUNS_10_0_COUNT_S2D', 'AWAY_DEF_RATING_S2D', 'HOME_PCT_AST_FGM_S2D']
    },
    {
        "name": "Overall_Rank2", "track": "Overall (Rank 2)",
        "features": ['AWAY_CHARGES_DRAWN_L5', 'HOME_PCT_PTS_3PT_L3', 'AWAY_DEF_RATING_L5', 'HOME_Q1_Q3_GAP_L5', 'HOME_PACE_L10', 'HOME_Q1_Q3_GAP_S2D', 'AWAY_FTA_RATE_L10', 'AWAY_MID_FREQ_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_TS_PCT_L10', 'AWAY_IS_B2B', 'HOME_PCT_AST_FGM_S2D']
    },
    {
        "name": "Overall_Rank3", "track": "Overall (Rank 3)",
        "features": ['AWAY_PACE_L3', 'HOME_Q1_Q3_GAP_L10', 'AWAY_DEF_RATING_L3', 'HOME_PCT_PTS_3PT_L3', 'HOME_PCT_AST_FGM_L5', 'HOME_AWAY_STREAK', 'HOME_Q1_Q3_GAP_L5', 'HOME_PACE_L10', 'HOME_Q1_Q3_GAP_S2D', 'HOME_PACE_S2D', 'HOME_CLUTCH_TS_PCT_L3', 'AWAY_FTA_RATE_L10', 'AWAY_MID_FREQ_L10', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_TS_PCT_L10', 'AWAY_IS_B2B', 'HOME_PCT_AST_FGM_S2D']
    },
    # ---------------- 絕對王者瀑布流 (6 Kings) ----------------
    {
        "name": "M062", "track": "King (50G 刺客)",
        "features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_L3', 'HOME_TS_PCT_L10', 'AWAY_DEF_RATING_L5', 'HOME_PCT_AST_FGM_L5', 'HOME_PCT_PTS_3PT_L3', 'HOME_NET_RATING_L10', 'AWAY_CHARGES_DRAWN_L5', 'AWAY_SCREEN_ASSISTS_L10', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'HOME_PACE_L5', 'HOME_TM_TOV_PCT_S2D', 'HOME_CHARGES_DRAWN_L10', 'HOME_DEF_RATING_S2D', 'AWAY_CLUTCH_TOV_PCT_L3', 'HOME_PACE_L10', 'HOME_MISSING_PIE_SUM', 'HOME_MISSING_PTS_SUM', 'HOME_MISSING_DEF_RATING_SUM_OPP']
    },
    {
        "name": "M079", "track": "King (70G 狙擊手)",
        "features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_L3', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L3', 'HOME_IS_B2B', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_AWAY_STREAK', 'AWAY_CLUTCH_TOV_PCT_L3', 'HOME_PACE_L10', 'HOME_MISSING_MIN_SUM', 'HOME_MISSING_USG_PCT_SUM_OPP', 'HOME_MISSING_PTS_SUM_OPP']
    },
    {
        "name": "M092", "track": "King (100G 主力)",
        "features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_CONTESTED_SHOTS_L10', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'AWAY_SCREEN_ASSISTS_S2D', 'HOME_TM_TOV_PCT_S2D', 'AWAY_PCT_PTS_3PT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_PACE_L10']
    },
    {
        "name": "M110", "track": "King (150G 重裝甲)",
        "features": ['AWAY_CONTESTED_SHOTS_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_PAINT_L5', 'HOME_PACE_L10', 'HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_MID_FREQ_S2D', 'AWAY_FTA_RATE_L3', 'HOME_EFG_PCT_L10', 'HOME_TM_TOV_PCT_S2D', 'HOME_PCT_PTS_3PT_L5', 'HOME_MOREYBALL_INDEX_L10', 'AWAY_CHARGES_DRAWN_L10', 'HOME_PCT_AST_FGM_L10', 'HOME_MAX_UNANSWERED_RUN_L5', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_DEF_RATING_L10', 'AWAY_PACE_S2D', 'HOME_CLUTCH_TS_PCT_S2D', 'AWAY_TM_TOV_PCT_S2D', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_TS_PCT_L10', 'AWAY_SCREEN_ASSISTS_L5', 'HOME_DEF_RATING_S2D', 'AWAY_PCT_PTS_3PT_L10', 'HOME_EFFICIENCY_TREND', 'HOME_MISSING_PIE_SUM', 'HOME_MISSING_USG_PCT_SUM_OPP']
    },
    {
        "name": "M126", "track": "King (200G 重砲)",
        "features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_CONTESTED_SHOTS_L10', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'AWAY_SCREEN_ASSISTS_S2D', 'HOME_TM_TOV_PCT_S2D', 'AWAY_PCT_PTS_3PT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_PACE_L10', 'HOME_MISSING_EFF_SUM']
    },
    {
        "name": "M014", "track": "King (最終防線)",
        "features": ['HOME_DEF_RATING_L5', 'HOME_Q1_Q3_GAP_L5', 'HOME_RUN_DEFICIT_RECOVERY_RATE_L5', 'HOME_REST_DAYS', 'HOME_Q1_Q3_GAP_L10', 'HOME_PACE_L10', 'HOME_Q1_Q3_GAP_S2D', 'HOME_CLUTCH_TS_PCT_L3', 'AWAY_MID_FREQ_L10', 'AWAY_IS_B2B', 'AWAY_CHARGES_DRAWN_L5', 'AWAY_Q1_Q3_GAP_L3', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_TS_PCT_L10', 'HOME_PCT_AST_FGM_S2D', 'AWAY_DEF_RATING_L3', 'HOME_PCT_PTS_3PT_L3', 'HOME_RUNS_10_0_COUNT_L3', 'AWAY_EFFICIENCY_TREND']
    }
]

def load_latest_features():
    print("🔍 正在從特徵大表提取各隊最新實力指標...")
    df_master = pd.read_csv(MASTER_FEATURES_CSV, low_memory=False)
    
    # 1. 統一轉大寫
    df_master.columns = [c.upper() for c in df_master.columns]

    # 🔥 2. 自動變形器：把大表的 _HOME, _AWAY 後綴轉換成 HOME_, AWAY_ 前綴！
    new_cols = []
    for c in df_master.columns:
        if c.endswith('_HOME'):
            new_cols.append('HOME_' + c[:-5])
        elif c.endswith('_AWAY'):
            new_cols.append('AWAY_' + c[:-5])
        else:
            new_cols.append(c)
    df_master.columns = new_cols
    
    df_master = df_master.fillna(0)
    
    # 取出各球隊「最新一場」的特徵數據
    latest_home = df_master.drop_duplicates(subset=['HOME_TEAM'], keep='last').copy()
    latest_away = df_master.drop_duplicates(subset=['AWAY_TEAM'], keep='last').copy()
    
    team_latest_home_stats = latest_home.set_index('HOME_TEAM').to_dict('index')
    team_latest_away_stats = latest_away.set_index('AWAY_TEAM').to_dict('index')
    
    return team_latest_home_stats, team_latest_away_stats

def predict_upcoming_games():
    print(f"🚀 啟動 NBA {len(ALL_MODELS)}神聯軍全預測系統！ (神級權重封印版)")
    
    if not os.path.exists(UPCOMING_CSV):
        print("❌ 找不到今日賽程 (upcoming_games.csv)！今日可能無賽事。")
        return
        
    upcoming_df = pd.read_csv(UPCOMING_CSV)
    if upcoming_df.empty:
        print("🤷‍♂️ 今日無賽事需要預測。")
        return
        
    home_stats_dict, away_stats_dict = load_latest_features()
    
    # ==========================================
    # 🔮 核心修改：讀取 .json 格式的封印大腦
    # ==========================================
    models = {}
    for stage in ALL_MODELS:
        m_name = stage['name']
        model_path = os.path.join(MODEL_DIR, f"{m_name}.json") # 改為讀取 .json
        if os.path.exists(model_path):
            model = XGBClassifier()
            model.load_model(model_path)
            models[m_name] = model
        else:
            print(f"⚠️ 警告: 找不到模型檔案 {model_path}，將跳過此模型的預測。")
    
    if not models:
        print("❌ 沒有任何可用的模型，預測中止。請確認 models/ 資料夾內有對應的 .json 檔案！")
        return

    predictions_log = []
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"\n🎯 今日共有 {len(upcoming_df)} 場賽事，開始進行 AI 全面分析...\n" + "="*60)
    
    for _, row in upcoming_df.iterrows():
        home_team = row['home_team']
        away_team = row['away_team']
        game_date = row['game_date']
        matchup_name = f"{away_team} @ {home_team}"
        
        home_features = home_stats_dict.get(home_team, {})
        away_features = away_stats_dict.get(away_team, {})
        
        # 建立今日賽事獨有上下文
        today_context = {
            "HOME_IS_B2B": 1 if row.get('home_is_b2b', False) else 0,
            "AWAY_IS_B2B": 1 if row.get('away_is_b2b', False) else 0,
            "HOME_REST_DAYS": row.get('home_rest_days', 2), 
            "AWAY_REST_DAYS": row.get('away_rest_days', 2)
        }
        
        # 統合所有的特徵進一個大字典
        X_input = {}
        X_input.update(home_features)
        X_input.update(away_features)
        X_input.update(today_context)
        
        print(f"🏀 {matchup_name}")
        
        # 讓清單上「所有成功載入的模型」都跑一次！
        for stage in ALL_MODELS:
            m_name = stage['name']
            if m_name not in models: continue
            
            features_list = stage['features']
            
            # 從 X_input 中精準萃取該模型需要的特徵，找不到就補 0
            X_model_dict = {f: X_input.get(f, 0) for f in features_list}
            
            # 強制轉換為 float32，保證雲端與本地二進位對齊
            X_df = pd.DataFrame([X_model_dict])[features_list].astype('float32')
            
            # 取得預測機率
            prob = models[m_name].predict_proba(X_df)[0]
            home_win_prob = prob[1]
            
            confidence = max(prob[0], prob[1])
            predicted_winner = home_team if home_win_prob >= 0.5 else away_team
            
            # 記錄預測結果
            prediction_record = {
                "Run_Time": run_timestamp,
                "Game_Date": game_date,
                "Matchup": matchup_name,
                "Model_Used": m_name,
                "Track_Name": stage['track'],
                "Predicted_Winner": predicted_winner,
                "Confidence_Pct": round(confidence * 100, 2)
            }
            predictions_log.append(prediction_record)
            
            # 印出該模型結果
            print(f"   📊 [{m_name:<15} | {stage['track']:<18}] 預測: {predicted_winner:<3} (信心: {round(confidence*100, 2)}%)")
            
        print("-" * 60)

    # ==========================================
    # 💾 智慧覆蓋與追加 (Upsert) 寫入 CSV
    # ==========================================
    if predictions_log:
        df_new = pd.DataFrame(predictions_log)
        os.makedirs(os.path.dirname(OUTPUT_PREDICTION), exist_ok=True)
        
        if os.path.exists(OUTPUT_PREDICTION):
            try:
                # 讀取既有紀錄
                df_history = pd.read_csv(OUTPUT_PREDICTION)
                # 將新舊資料合併
                df_combined = pd.concat([df_history, df_new], ignore_index=True)
                # 日期 + 對戰 + 模型名稱 作為唯一鍵值去重
                df_combined = df_combined.drop_duplicates(subset=['Game_Date', 'Matchup', 'Model_Used'], keep='last')
            except Exception as e:
                print(f"⚠️ 讀取歷史紀錄失敗，將直接覆蓋: {e}")
                df_combined = df_new
        else:
            df_combined = df_new
            
        # 覆蓋寫入
        df_combined.to_csv(OUTPUT_PREDICTION, index=False, encoding='utf-8-sig')
        
        print(f"\n✅ 今日預測完畢！總計 {len(predictions_log)} 筆預測結果已成功【更新/追加】至: {OUTPUT_PREDICTION}")

if __name__ == "__main__":
    predict_upcoming_games()