import pandas as pd
import numpy as np
import os
import sqlite3
import warnings
from datetime import datetime
from xgboost import XGBClassifier

warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ 設定與路徑區
# ==========================================
UPCOMING_CSV = 'data/upcoming_games.csv'
MASTER_FEATURES_CSV = 'data/ml_features_master.csv'
DB_PATH = 'data/nba_current.db' # 👈 新增：資料庫路徑
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

# ==========================================
# 🥇 賽後自動結算系統 (精準對獎引擎)
# ==========================================
def settle_past_predictions():
    print("\n" + "="*60)
    print("🔄 啟動賽後自動結算系統 (精準對獎引擎)...")
    
    if not os.path.exists(OUTPUT_PREDICTION):
        print("   ℹ️ 尚未產生任何預測紀錄檔，跳過結算。")
        return

    # 讀取預測紀錄
    preds_df = pd.read_csv(OUTPUT_PREDICTION)
    
    # 初始化防呆欄位
    if 'Is_Win' not in preds_df.columns:
        preds_df['Is_Win'] = np.nan
    if 'Line' not in preds_df.columns:
        preds_df['Line'] = '未開盤'

    unsettled_mask = preds_df['Is_Win'].isna()
    missing_line_mask = preds_df['Line'].isna() | (preds_df['Line'] == '未開盤') | (preds_df['Line'] == '') | (preds_df['Line'] == 'nan')
    
    if not unsettled_mask.any() and not missing_line_mask.any():
        print("   ✅ 所有歷史預測皆已結算且盤口完整！")
        return

    # 🚀 步驟 1：只從資料庫抽取「比分」與「備用盤口」
    actual_scores = {}
    if os.path.exists(DB_PATH):
        try:
            conn = sqlite3.connect(DB_PATH)
            games_df = pd.read_sql("SELECT * FROM games", conn)
            conn.close()
            
            games_df.columns = [c.lower() for c in games_df.columns]
            games_df['date_str'] = pd.to_datetime(games_df['game_date']).dt.strftime('%Y-%m-%d')
            
            for _, row in games_df.iterrows():
                matchup = f"{row.get('away_team')} @ {row.get('home_team')}"
                key = f"{matchup}_{row['date_str']}"
                
                # 安全抽取數值
                actual_scores[key] = {
                    'h_score': pd.to_numeric(row.get('home_score'), errors='coerce'),
                    'a_score': pd.to_numeric(row.get('away_score'), errors='coerce'),
                    # 如果有 tw_spread_score 就用，沒有就試試看 vegas_spread
                    'db_line': pd.to_numeric(row.get('tw_spread_score', row.get('vegas_spread')), errors='coerce')
                }
        except Exception as e:
            print(f"   ⚠️ 讀取資料庫 games 表格時發生錯誤: {e}")
    else:
        print(f"   ⚠️ 找不到資料庫 {DB_PATH}，無法進行深度挖掘結算。")
        return
    
    settled_count = 0
    fixed_line_count = 0

    # 🚀 步驟 2：結合 CSV 的盤口進行派彩
    for idx, row in preds_df.iterrows():
        current_line = str(row.get('Line', '')).strip()
        is_win_val = row.get('Is_Win')
        
        # 如果已經結算過，且盤口也不是未開盤，就跳過
        if pd.notna(is_win_val) and current_line not in ['', 'nan', 'NaN', '未開盤', 'None']:
            continue

        try:
            pred_date_str = pd.to_datetime(str(row['Game_Date']).strip()).strftime('%Y-%m-%d')
        except:
            pred_date_str = str(row['Game_Date']).strip()

        key = f"{row['Matchup']}_{pred_date_str}"
        matched_data = actual_scores.get(key)

        # 時區容錯：找前後一天
        if not matched_data:
            try:
                pred_dt = pd.to_datetime(pred_date_str)
                key_prev = f"{row['Matchup']}_{(pred_dt - pd.Timedelta(days=1)).strftime('%Y-%m-%d')}"
                key_next = f"{row['Matchup']}_{(pred_dt + pd.Timedelta(days=1)).strftime('%Y-%m-%d')}"
                matched_data = actual_scores.get(key_prev) or actual_scores.get(key_next)
            except:
                pass

        if matched_data:
            h_score = matched_data['h_score']
            a_score = matched_data['a_score']
            
            # 🔥 防禦機制：如果資料庫裡這場比賽的比分是 NaN 或 0，代表根本還沒打完，直接跳過！
            if pd.isna(h_score) or pd.isna(a_score) or h_score == 0 or a_score == 0:
                continue
                
            # 🎯 決定用來對獎的盤口數字 (超級關鍵！)
            final_line = np.nan
            try:
                # 優先使用 CSV 當時抓到的真實盤口
                if current_line not in ['', 'nan', 'NaN', '未開盤', 'None']:
                    final_line = float(current_line)
                # 如果 CSV 沒抓到，才去用資料庫補的盤口
                elif pd.notna(matched_data['db_line']):
                    final_line = float(matched_data['db_line'])
            except:
                pass

            # 如果連資料庫裡都沒有盤口數字，真的無能為力了，跳過
            if pd.isna(final_line):
                continue
                
            # 如果當初沒盤口，現在用資料庫補上了，寫入 CSV
            if current_line in ['', 'nan', 'NaN', '未開盤', 'None'] and pd.notna(final_line):
                preds_df.at[idx, 'Line'] = final_line
                fixed_line_count += 1
            
            # 🏆 派彩結算邏輯
            if pd.isna(is_win_val):
                try:
                    away_team, home_team = str(row['Matchup']).split(' @ ')
                    diff = h_score - a_score + final_line
                    
                    if diff > 0:
                        spread_winner = home_team
                    elif diff < 0:
                        spread_winner = away_team
                    else:
                        spread_winner = "PUSH" # 走水
                        
                    # 只有在非走水的情況下才判定勝負
                    if spread_winner != "PUSH":
                        preds_df.at[idx, 'Is_Win'] = 1 if str(row['Predicted_Winner']).strip() == spread_winner else 0
                        settled_count += 1
                except:
                    pass

    if settled_count > 0 or fixed_line_count > 0:
        preds_df.to_csv(OUTPUT_PREDICTION, index=False, encoding='utf-8-sig')
        print(f"   💰 結算完成！成功派彩了 {settled_count} 筆賽果，並回填了 {fixed_line_count} 筆盤口數字。")
    else:
        print("   ⏳ 尚無最新賽果可供結算 (提示：請確認你是否已經更新了包含昨日『完賽比分』的資料庫)。")
    print("="*60)

# ==========================================
# 🔍 載入特徵與預測核心
# ==========================================
def load_latest_features():
    print("🔍 正在從特徵大表提取各隊最新實力指標...")
    df_master = pd.read_csv(MASTER_FEATURES_CSV, low_memory=False)
    
    # 1. 統一轉大寫
    df_master.columns = [c.upper() for c in df_master.columns]

    # 2. 自動變形器：把大表的 _HOME, _AWAY 後綴轉換成 HOME_, AWAY_ 前綴！
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
    print(f"🚀 啟動 NBA {len(ALL_MODELS)}神聯軍全預測系統！ (神級防呆版)")
    
    if not os.path.exists(UPCOMING_CSV):
        print("❌ 找不到今日賽程 (upcoming_games.csv)！今日可能無賽事。")
        return
        
    upcoming_df = pd.read_csv(UPCOMING_CSV)
    if upcoming_df.empty:
        print("🤷‍♂️ 今日無賽事需要預測。")
        return
        
    home_stats_dict, away_stats_dict = load_latest_features()
    
    # 讀取 .json 格式的封印大腦
    models = {}
    for stage in ALL_MODELS:
        m_name = stage['name']
        model_path = os.path.join(MODEL_DIR, f"{m_name}.json")
        if os.path.exists(model_path):
            model = XGBClassifier()
            model.load_model(model_path)
            models[m_name] = model
        else:
            print(f"⚠️ 警告: 找不到模型檔案 {model_path}，將跳過此模型的預測。")
    
    if not models:
        print("❌ 沒有任何可用的模型，預測中止。")
        return

    predictions_log = []
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"\n🎯 今日共有 {len(upcoming_df)} 場賽事，開始進行 AI 全面分析...\n" + "="*60)
    
    for _, row in upcoming_df.iterrows():
        # 🔥 強制將欄位名稱去空白並轉大寫 (防禦隱形空白殺手)
        row_upper = {str(k).strip().upper(): v for k, v in row.items()}
        
        home_team = row_upper.get('HOME_TEAM')
        away_team = row_upper.get('AWAY_TEAM')
        game_date = row_upper.get('GAME_DATE')
        matchup_name = f"{away_team} @ {home_team}"
        
        home_features = home_stats_dict.get(home_team, {})
        away_features = away_stats_dict.get(away_team, {})
        
        # 🎯 擴大搜尋範圍，精準抓取維加斯讓分盤口 VEGAS_SPREAD
        possible_spread_cols = ['VEGAS_SPREAD', 'TW_SPREAD_SCORE', 'TW_SPREAD', 'SPREAD', 'LINE']
        game_line = '未開盤'
        for col in possible_spread_cols:
            if col in row_upper:
                val = row_upper[col]
                # 確保不是空值、不是 nan 字串
                if pd.notna(val) and str(val).strip() not in ['', 'nan', 'NaN', 'None']:
                    game_line = str(val).strip()
                    break
        
        today_context = {
            "HOME_IS_B2B": 1 if str(row_upper.get('HOME_IS_B2B', 'False')).strip().lower() == 'true' else 0,
            "AWAY_IS_B2B": 1 if str(row_upper.get('AWAY_IS_B2B', 'False')).strip().lower() == 'true' else 0,
            "HOME_REST_DAYS": row_upper.get('HOME_REST_DAYS', 2), 
            "AWAY_REST_DAYS": row_upper.get('AWAY_REST_DAYS', 2)
        }
        
        X_input = {}
        
        # 🔥 嚴格過濾：主隊只拿 HOME 特徵，客隊只拿 AWAY 特徵
        for k, v in home_features.items():
            if k.startswith('HOME_'):
                X_input[k] = v
                
        for k, v in away_features.items():
            if k.startswith('AWAY_'):
                X_input[k] = v
                
        X_input.update(today_context)
        
        print(f"🏀 {matchup_name} (讓分盤口: {game_line})")
        
        for stage in ALL_MODELS:
            m_name = stage['name']
            if m_name not in models: continue
            
            features_list = stage['features']
            X_model_dict = {f: X_input.get(f, 0) for f in features_list}
            X_df = pd.DataFrame([X_model_dict])[features_list].astype('float32')
            
            model_obj = models[m_name]
            prob = model_obj.predict_proba(X_df)[0]
            
            # 🔥 防呆 4：動態尋找主隊或 1 的索引，避免 XGBoost 字母排序反轉
            classes = list(model_obj.classes_)
            if home_team in classes:
                home_idx = classes.index(home_team)
            elif 1 in classes:
                home_idx = classes.index(1)
            else:
                home_idx = 1 # Fallback
                
            home_win_prob = prob[home_idx]
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
                "Is_Win": np.nan, # 新預測的單子，預設為未結算
                "Line": game_line # 👈 新增：把讓分盤數字寫入 CSV
            }
            predictions_log.append(prediction_record)
            
            # 🔥 視覺化門檻提示 (大於 53% 標示火焰)
            conf_pct = round(confidence * 100, 2)
            is_overall = ('OVERALL' in str(stage['track']).upper() or 'M014' in m_name)
            
            if is_overall:
                action_tag = "👉 [全覆蓋推]"
            elif conf_pct >= 53.0:
                action_tag = "🔥 [重注狙擊]"
            else:
                action_tag = "👀 [觀望放棄]"
                
            print(f"   📊 [{m_name:<15} | {stage['track']:<18}] 預測: {predicted_winner:<3} (信心: {conf_pct:>5}%) {action_tag}")
            
        print("-" * 60)

    # ==========================================
    # 💾 智慧覆蓋與追加 (Upsert) 寫入 CSV
    # ==========================================
    if predictions_log:
        df_new = pd.DataFrame(predictions_log)
        os.makedirs(os.path.dirname(OUTPUT_PREDICTION), exist_ok=True)
        
        if os.path.exists(OUTPUT_PREDICTION):
            try:
                df_history = pd.read_csv(OUTPUT_PREDICTION)
                df_combined = pd.concat([df_history, df_new], ignore_index=True)
                # 以 日期+對戰+模型名稱 作為唯一鍵值，若重複則用最新預測覆蓋
                df_combined = df_combined.drop_duplicates(subset=['Game_Date', 'Matchup', 'Model_Used'], keep='last')
            except Exception as e:
                print(f"⚠️ 讀取歷史紀錄失敗: {e}")
                df_combined = df_new
        else:
            df_combined = df_new
            
        df_combined.to_csv(OUTPUT_PREDICTION, index=False, encoding='utf-8-sig')
        print(f"\n✅ 今日預測完畢！總計 {len(predictions_log)} 筆預測結果已成功【更新/追加】至: {OUTPUT_PREDICTION}")

if __name__ == "__main__":
    # 1. 先執行結算，把過去的單子對獎 (順便去資料庫回填漏抓的 Line 盤口)
    settle_past_predictions()
    
    # 2. 再執行今日賽事預測 (嚴格從 upcoming_games.csv 抓盤口)
    predict_upcoming_games()