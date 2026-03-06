import pandas as pd
import numpy as np
import os
import joblib
import warnings
from datetime import datetime
import json
import sqlite3

warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ 設定與路徑區
# ==========================================
UPCOMING_CSV = 'data/upcoming_games.csv'
MASTER_FEATURES_CSV = 'data/ml_features_master.csv'
DB_PATH = 'data/nba_current.db'
if not os.path.exists(DB_PATH): DB_PATH = 'data/nba_raw.db'
MODEL_DIR = 'models/'  
OUTPUT_PREDICTION = 'data/predictions_history_log.csv' 

# ==========================================
# 🏆 24 神聯軍全特徵定義 (純淨版，已剔除 TEAM_ID 毒瘤)
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

# ==========================================
# 🚑 核心功能：計算今日傷兵的真實影響力 (MISSING_PIE_SUM 等)
# ==========================================
def calculate_missing_stats(injured_ids_json):
    """讀取 JSON 傷兵名單，去資料庫找他們近 20 場的平均數據並加總"""
    if not injured_ids_json or pd.isna(injured_ids_json): return {}, 0
    try:
        injured_list = json.loads(injured_ids_json)
    except: return {}, 0

    totals = {'PIE': 0, 'PTS': 0, 'DEF_RATING': 0, 'MIN': 0, 'USG_PCT': 0, 'EFF': 0}
    if not injured_list: return totals, 0

    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        count_injured = 0
        for p in injured_list:
            pid = p.get('id')
            if not pid: continue
            
            # 使用 Pandas 去除錯，避免 SQLite 因欄位不存在而當機
            query = f"""
            SELECT a.PIE, b.PTS, a.DEF_RATING, a.USG_PCT, b.REB, b.AST, b.STL, b.BLK, b.FGA, b.FGM, b.FTA, b.FTM, b.MIN, b.*
            FROM boxscore_advanced a
            JOIN boxscore_base b ON a.GAME_ID = b.GAME_ID AND a.PLAYER_ID = b.PLAYER_ID
            WHERE a.PLAYER_ID = {pid} AND b.MIN IS NOT NULL AND b.MIN != '0:00' AND b.MIN != '0'
            ORDER BY a.GAME_DATE DESC
            LIMIT 20
            """
            df = pd.read_sql(query, conn)
            
            if not df.empty:
                # 計算分鐘數轉換
                df['MIN_NUM'] = df['MIN'].apply(lambda x: float(str(x).split(':')[0]) + float(str(x).split(':')[1])/60.0 if ':' in str(x) else float(x))
                # 取得失誤欄位 (NBA 官方有時叫 TO, 有時叫 TOV)
                to_val = df['TO'] if 'TO' in df.columns else (df['TOV'] if 'TOV' in df.columns else 0)
                # 計算 EFF 效率值
                df['EFF'] = df['PTS'] + df['REB'] + df['AST'] + df['STL'] + df['BLK'] - (df['FGA'] - df['FGM']) - (df['FTA'] - df['FTM']) - to_val
                
                totals['PIE'] += df['PIE'].mean() or 0
                totals['PTS'] += df['PTS'].mean() or 0
                totals['DEF_RATING'] += df['DEF_RATING'].mean() or 0
                totals['USG_PCT'] += df['USG_PCT'].mean() or 0
                totals['EFF'] += df['EFF'].mean() or 0
                totals['MIN'] += df['MIN_NUM'].mean() or 0
                count_injured += 1
                
        conn.close()
        return totals, count_injured
    except Exception as e:
        return totals, 0

def load_latest_features():
    print("🔍 正在從特徵大表提取各隊最新實力指標...")
    df_master = pd.read_csv(MASTER_FEATURES_CSV, low_memory=False)
    df_master = df_master.fillna(0)
    
    df_master.columns = [c.upper() for c in df_master.columns]
    latest_home = df_master.drop_duplicates(subset=['HOME_TEAM'], keep='last').copy()
    latest_away = df_master.drop_duplicates(subset=['AWAY_TEAM'], keep='last').copy()
    
    team_latest_home_stats = latest_home.set_index('HOME_TEAM').to_dict('index')
    team_latest_away_stats = latest_away.set_index('AWAY_TEAM').to_dict('index')
    
    return team_latest_home_stats, team_latest_away_stats

def predict_upcoming_games():
    print(f"🚀 啟動 NBA {len(ALL_MODELS)}神聯軍全預測系統！ (全軍出擊模式)")
    
    if not os.path.exists(UPCOMING_CSV):
        print("❌ 找不到今日賽程 (upcoming_games.csv)！今日可能無賽事。")
        return
        
    upcoming_df = pd.read_csv(UPCOMING_CSV)
    if upcoming_df.empty:
        print("🤷‍♂️ 今日無賽事需要預測。")
        return
        
    home_stats_dict, away_stats_dict = load_latest_features()
    
    models = {}
    for stage in ALL_MODELS:
        m_name = stage['name']
        model_path = os.path.join(MODEL_DIR, f"{m_name}.pkl")
        if os.path.exists(model_path):
            models[m_name] = joblib.load(model_path)
        else:
            print(f"⚠️ 警告: 找不到模型檔案 {model_path}")
    
    if not models:
        print("❌ 沒有任何可用的模型，預測中止。請確認已執行 train_deploy.py！")
        return

    predictions_log = []
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"\n🎯 今日共有 {len(upcoming_df)} 場賽事，開始進行 AI 全面分析...\n" + "="*60)
    
    for _, row in upcoming_df.iterrows():
        home_team = row['home_team']
        away_team = row['away_team']
        game_date = row['game_date']
        
        # 賠率資訊 (若無則顯示盤口未開)
        spread = row.get('vegas_spread', 0.0)
        total = row.get('vegas_total', 0.0)
        spread_text = f"{home_team} 讓 {abs(spread)}" if spread < 0 else (f"{away_team} 讓 {spread}" if spread > 0 else "PK盤")
        if spread == 0.0 and total == 0.0: spread_text = "目前無盤口"
        
        # 🚑 抓取並計算傷兵折損
        home_missing, h_count = calculate_missing_stats(row.get('home_injuries_ids'))
        away_missing, a_count = calculate_missing_stats(row.get('away_injuries_ids'))

        matchup_name = f"{away_team} @ {home_team}"
        print(f"🏀 {matchup_name} | 🎰 國際盤口: {spread_text} (大小 {total})")
        print(f"   🚑 傷兵影響: {home_team} 缺席 {h_count} 人 (折損 PIE: {home_missing.get('PIE', 0):.3f}) | {away_team} 缺席 {a_count} 人 (折損 PIE: {away_missing.get('PIE', 0):.3f})")
        
        home_features = home_stats_dict.get(home_team, {})
        away_features = away_stats_dict.get(away_team, {})
        
        # 將賽事當前狀態與傷病分數整理給模型
        today_context = {
            "HOME_IS_B2B": 1 if row.get('home_is_b2b', False) else 0,
            "AWAY_IS_B2B": 1 if row.get('away_is_b2b', False) else 0,
            
            # 主隊折損
            "HOME_MISSING_PIE_SUM": home_missing.get('PIE', 0),
            "HOME_MISSING_PTS_SUM": home_missing.get('PTS', 0),
            "HOME_MISSING_DEF_RATING_SUM": home_missing.get('DEF_RATING', 0),
            "HOME_MISSING_USG_PCT_SUM": home_missing.get('USG_PCT', 0),
            "HOME_MISSING_EFF_SUM": home_missing.get('EFF', 0),
            "HOME_MISSING_MIN_SUM": home_missing.get('MIN', 0),

            # 客隊折損
            "AWAY_MISSING_PIE_SUM": away_missing.get('PIE', 0),
            "AWAY_MISSING_PTS_SUM": away_missing.get('PTS', 0),
            "AWAY_MISSING_DEF_RATING_SUM": away_missing.get('DEF_RATING', 0),
            "AWAY_MISSING_USG_PCT_SUM": away_missing.get('USG_PCT', 0),
            "AWAY_MISSING_EFF_SUM": away_missing.get('EFF', 0),
            "AWAY_MISSING_MIN_SUM": away_missing.get('MIN', 0),

            # 針對 _OPP 特徵：對手(客隊)的折損就是主隊眼中的 _OPP
            "HOME_MISSING_DEF_RATING_SUM_OPP": away_missing.get('DEF_RATING', 0),
            "HOME_MISSING_USG_PCT_SUM_OPP": away_missing.get('USG_PCT', 0),
            "HOME_MISSING_PTS_SUM_OPP": away_missing.get('PTS', 0),
            "HOME_MISSING_PIE_SUM_OPP": away_missing.get('PIE', 0),

            # 客隊眼中的 _OPP 則是主隊的折損
            "AWAY_MISSING_DEF_RATING_SUM_OPP": home_missing.get('DEF_RATING', 0),
            "AWAY_MISSING_USG_PCT_SUM_OPP": home_missing.get('USG_PCT', 0),
            "AWAY_MISSING_PTS_SUM_OPP": home_missing.get('PTS', 0),
            "AWAY_MISSING_PIE_SUM_OPP": home_missing.get('PIE', 0),
        }
        
        for stage in ALL_MODELS:
            m_name = stage['name']
            if m_name not in models: continue
            
            X_input = {}
            for feat in stage['features']:
                feat_upper = feat.upper()
                if feat_upper in today_context:
                    X_input[feat_upper] = today_context[feat_upper]
                elif feat_upper.startswith('HOME_'):
                    X_input[feat_upper] = home_features.get(feat_upper, 0)
                elif feat_upper.startswith('AWAY_'):
                    X_input[feat_upper] = away_features.get(feat_upper, 0)
                else:
                    X_input[feat_upper] = 0
            
            X_df = pd.DataFrame([X_input])
            
            # 取得預測機率
            prob = models[m_name].predict_proba(X_df)[0]
            home_win_prob = prob[1]
            confidence = max(prob[0], prob[1])
            predicted_winner = home_team if home_win_prob >= 0.5 else away_team
            
            prediction_record = {
                "Run_Time": run_timestamp,
                "Game_Date": game_date,
                "Matchup": matchup_name,
                "Model_Used": m_name,
                "Track_Name": stage['track'],
                "Predicted_Winner": predicted_winner,
                "Confidence_Pct": round(confidence * 100, 2),
                "Vegas_Spread": spread,
                "Vegas_Total": total
            }
            predictions_log.append(prediction_record)
            
            print(f"   📊 [{m_name:<15} | {stage['track']:<18}] 預測: {predicted_winner:<3} (信心: {round(confidence*100, 2):>5}%)")
            
        print("-" * 60)

    # ==========================================
    # 💾 寫入 CSV
    # ==========================================
    if predictions_log:
        df_new = pd.DataFrame(predictions_log)
        os.makedirs(os.path.dirname(OUTPUT_PREDICTION), exist_ok=True)
        
        if os.path.exists(OUTPUT_PREDICTION):
            try:
                df_history = pd.read_csv(OUTPUT_PREDICTION)
                df_combined = pd.concat([df_history, df_new], ignore_index=True)
                df_combined = df_combined.drop_duplicates(subset=['Game_Date', 'Matchup', 'Model_Used'], keep='last')
            except:
                df_combined = df_new
        else:
            df_combined = df_new
            
        df_combined.to_csv(OUTPUT_PREDICTION, index=False, encoding='utf-8-sig')
        print(f"\n✅ 今日預測完畢！總計 {len(predictions_log)} 筆預測結果已成功【更新/追加】至: {OUTPUT_PREDICTION}")

if __name__ == "__main__":
    predict_upcoming_games()