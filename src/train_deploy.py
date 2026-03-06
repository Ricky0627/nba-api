import pandas as pd
import numpy as np
import os
import joblib
import warnings
from xgboost import XGBClassifier
from prepare_data import get_merged_dataframe

warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ 設定與路徑區
# ==========================================
MASTER_FEATURES_CSV = 'data/ml_features_master.csv'
MODEL_DIR = 'models/'

# ==========================================
# 🏆 24 神聯軍全特徵定義 (與預測腳本完全一致)
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
        "features": ['HOME_LOOSE_BALLS_RECOVERED_S2D', 'AWAY_CONTESTED_SHOTS_L10', 'HOME_TS_PCT_L10', 'HOME_PCT_AST_FGM_L5', 'HOME_PACE_S2D', 'HOME_PCT_PTS_3PT_L3', 'AWAY_PCT_PTS_PAINT_L5', 'HOME_CLUTCH_TS_PCT_S2D', 'HOME_EFG_PCT_L10', 'AWAY_SCREEN_ASSISTS_S2D', 'HOME_TM_TOV_PCT_S2D', 'AWAY_PCT_PTS_3PT_L10', 'AWAY_RUNS_10_0_COUNT_L3', 'HOME_RUNS_10_0_COUNT_S2D', 'HOME_PACE_L10', 'HOME_TEAM_ID', 'HOME_MISSING_EFF_SUM']
    },
    {
        "name": "M014", "track": "King (最終防線)",
        "features": ['HOME_DEF_RATING_L5', 'HOME_Q1_Q3_GAP_L5', 'HOME_RUN_DEFICIT_RECOVERY_RATE_L5', 'HOME_REST_DAYS', 'HOME_Q1_Q3_GAP_L10', 'HOME_PACE_L10', 'HOME_Q1_Q3_GAP_S2D', 'HOME_CLUTCH_TS_PCT_L3', 'AWAY_MID_FREQ_L10', 'AWAY_IS_B2B', 'AWAY_CHARGES_DRAWN_L5', 'AWAY_Q1_Q3_GAP_L3', 'HOME_PCT_PTS_PAINT_L5', 'AWAY_TS_PCT_L10', 'HOME_PCT_AST_FGM_S2D', 'AWAY_DEF_RATING_L3', 'HOME_PCT_PTS_3PT_L3', 'HOME_RUNS_10_0_COUNT_L3', 'AWAY_EFFICIENCY_TREND']
    }
]

def load_training_data():
    print("📥 1. 正在載入歷史特徵大表與比賽結果...")
    
    if not os.path.exists(MASTER_FEATURES_CSV):
        raise FileNotFoundError(f"找不到特徵大表: {MASTER_FEATURES_CSV}")
        
    df_master = pd.read_csv(MASTER_FEATURES_CSV, low_memory=False)
    df_master.columns = [c.upper() for c in df_master.columns]
    
    # 填補空值以防報錯
    df_master = df_master.fillna(0)
    
    # ---------------------------------------------------------
    # 🎯 建立預測目標 (Target): 主隊是否獲勝 (HOME_WIN = 1 或 0)
    # ---------------------------------------------------------
    # 我們從 boxscore_base 來抓取比賽勝負結果
    df_base = get_merged_dataframe("boxscore_base")
    df_base.columns = [c.upper() for c in df_base.columns]
    
    # 只抓取主隊的數據列 (MATCHUP 包含 ' vs. ' 代表主場)
    df_home = df_base[df_base['MATCHUP'].str.contains(' vs. ', na=False)].drop_duplicates('GAME_ID')
    
    # 建立勝負標籤 (PLUS_MINUS 大於 0 代表贏球)
    if 'PLUS_MINUS' in df_home.columns:
        df_home['HOME_WIN'] = (df_home['PLUS_MINUS'] > 0).astype(int)
    elif 'WL' in df_home.columns:
        df_home['HOME_WIN'] = (df_home['WL'] == 'W').astype(int)
    else:
        raise ValueError("無法在 boxscore_base 中找到勝負依據 (PLUS_MINUS 或 WL)！")
        
    # 將標籤合併回特徵大表
    df_home['GAME_ID'] = df_home['GAME_ID'].astype(str).str.zfill(10)
    df_master['GAME_ID'] = df_master['GAME_ID'].astype(str).str.zfill(10)
    
    df_train = df_master.merge(df_home[['GAME_ID', 'HOME_WIN']], on='GAME_ID', how='inner')
    
    print(f"   📊 成功配對 {len(df_train)} 場有效歷史賽事用於訓練。")
    return df_train

def train_and_deploy_models(df_train):
    print("\n🚀 2. 啟動 XGBoost 兵工廠，開始鍛造 24 把神兵利器...")
    os.makedirs(MODEL_DIR, exist_ok=True)
    
    for stage in ALL_MODELS:
        m_name = stage['name']
        features = stage['features']
        
        # 防呆檢查：確保所有要求的特徵都在表裡面，如果沒有就補 0
        missing_cols = [f for f in features if f not in df_train.columns]
        for col in missing_cols:
            df_train[col] = 0
            
        X = df_train[features]
        y = df_train['HOME_WIN']
        
        # XGBoost 參數設定 (通用高勝率設定)
        model = XGBClassifier(
            n_estimators=120,       # 樹的數量
            learning_rate=0.05,     # 學習率
            max_depth=4,            # 樹的深度 (避免過擬合)
            subsample=0.8,          # 隨機抽取 80% 樣本訓練
            colsample_bytree=0.8,   # 隨機抽取 80% 特徵訓練
            random_state=42,        # 固定亂數種子，保證每次訓練結果一致
            eval_metric='logloss'
        )
        
        # 開始訓練
        model.fit(X, y)
        
        # 儲存模型為 .pkl 檔
        model_path = os.path.join(MODEL_DIR, f"{m_name}.pkl")
        joblib.dump(model, model_path)
        
        print(f"   ✅ [{m_name:<12}] 訓練完成並已部署！ (特徵數: {len(features)})")
        
    print(f"\n🎉 恭喜！24 個 MLOps 模型已全數部署至 {MODEL_DIR} 目錄！")
    print("👉 現在你可以安心執行 predict_today.py 來進行賽事預測了！")

if __name__ == "__main__":
    try:
        df_dataset = load_training_data()
        train_and_deploy_models(df_dataset)
    except Exception as e:
        print(f"❌ 訓練管線發生錯誤: {e}")