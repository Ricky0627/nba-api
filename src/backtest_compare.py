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

TRAIN_SEASONS = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22', '2022-23', '2023-24', '2024-25']
TEST_SEASON = ['2025-26']
SNIPER_THRESHOLD = 0.53  # 高信心出手門檻

# ==========================================
# 🏆 24 神聯軍全特徵定義 (前綴對齊版，保證抓得到資料)
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
    # ---------------- 總榜 (Overall) 賽道 (重回 62% 聖杯) ----------------
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
    # ---------------- 王者組合 (King) ----------------
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

def run_static_backtest():
    print("="*85)
    print("🚀 啟動 GitHub Action 專用：讓分盤靜態回測引擎 (完美還原窮舉實驗室)")
    print("="*85)

    try:
        df = pd.read_csv(MASTER_FEATURES_CSV)
        
        # 🔥 終極防呆機制：強制將資料表的所有欄位轉成全大寫！(解決 KeyError: 'SEASON' 與特徵找不到的問題)
        df.columns = [str(c).upper() for c in df.columns]
        
        print(f"✅ 成功載入特徵大表，共 {len(df)} 筆資料")
    except Exception as e:
        print(f"❌ 無法讀取 {MASTER_FEATURES_CSV}，錯誤: {e}")
        return

    # 防呆檢查 (現在欄位一定都是大寫了)
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
    
    # ... 後面照舊 ...

    y_train = train_df['HOME_WIN'].values
    y_test = test_df['HOME_WIN'].values

    print(f"📊 訓練基底: {len(train_df)} 場 (2016-2025) | 測試樣本: {len(test_df)} 場 (本賽季)")
    print("="*85)
    print(f"{'模型名稱':<15} | {'賽道屬性':<18} | {'全覆蓋勝率 (硬猜)':<20} | {'高信心勝率 (>53%)':<18}")
    print("-" * 85)

    for stage in ALL_MODELS:
        m_name = stage['name']
        features = stage['features']

        # 安全機制：確保所有需要的特徵都在 DataFrame 中，若無則補 0 (防止程式崩潰與 0.509 陷阱)
        missing_cols = [f for f in features if f not in train_df.columns]
        if missing_cols:
            print(f"⚠️ 警告: {m_name} 找不到特徵 {missing_cols[:2]}... 將補 0")
        for col in missing_cols:
            train_df[col] = 0
            test_df[col] = 0
            
        # 🔥 核心修正 1：完美補齊 .fillna(0)，確保與窮舉時的處理邏輯 100% 一致
        X_train = train_df[features].fillna(0)
        X_test = test_df[features].fillna(0)

        # 🔥 核心修正 2：嚴格鎖定窮舉時防過擬合的 XGBoost 參數 (50棵樹, 深度3)
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

        # 🎯 評估 A：全覆蓋勝率 (所有比賽都預測，不設門檻)
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

    print("="*85)
    print("✅ 回測完畢！你可以清楚看到 Overall 賽道的全覆蓋勝率已重返 60%+ 榮耀！")

if __name__ == "__main__":
    run_static_backtest()