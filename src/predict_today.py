import pandas as pd
import numpy as np
import os
import joblib
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ 設定與路徑區
# ==========================================
UPCOMING_CSV = 'data/upcoming_games.csv'
MASTER_FEATURES_CSV = 'data/ml_features_master.csv'
MODEL_DIR = 'models/'  # 假設你的訓練腳本把模型存在這裡
OUTPUT_PREDICTION = 'data/today_predictions.csv'

# ==========================================
# 🏆 絕對王者瀑布流特徵定義 (對應你提供的交接清單)
# ==========================================
WATERFALL_MODELS = [
    {
        "name": "M062", "track": "50G (第一順位刺客)", "threshold": 0.58,
        "features": [
            "HOME_LOOSE_BALLS_RECOVERED_S2D", "AWAY_MID_FREQ_L3", "HOME_TS_PCT_L10", "AWAY_DEF_RATING_L5",
            "HOME_PCT_AST_FGM_L5", "HOME_PCT_PTS_3PT_L3", "HOME_NET_RATING_L10", "AWAY_CHARGES_DRAWN_L5",
            "AWAY_SCREEN_ASSISTS_L10", "HOME_CLUTCH_TS_PCT_S2D", "HOME_EFG_PCT_L10", "HOME_PACE_L5",
            "HOME_TM_TOV_PCT_S2D", "HOME_CHARGES_DRAWN_L10", "HOME_DEF_RATING_S2D", "AWAY_CLUTCH_TOV_PCT_L3",
            "HOME_PACE_L10", "HOME_MISSING_PIE_SUM", "HOME_MISSING_PTS_SUM", "HOME_MISSING_DEF_RATING_SUM_OPP"
        ]
    },
    {
        "name": "M079", "track": "70G (第二順位狙擊手)", "threshold": 0.56,
        "features": [
            "HOME_LOOSE_BALLS_RECOVERED_S2D", "AWAY_MID_FREQ_L3", "HOME_TS_PCT_L10", "HOME_PCT_AST_FGM_L5",
            "HOME_PACE_S2D", "HOME_PCT_PTS_3PT_L3", "HOME_IS_B2B", "HOME_CLUTCH_TS_PCT_S2D",
            "HOME_EFG_PCT_L10", "HOME_TM_TOV_PCT_S2D", "HOME_AWAY_STREAK", "AWAY_CLUTCH_TOV_PCT_L3",
            "HOME_PACE_L10", "HOME_MISSING_MIN_SUM", "HOME_MISSING_USG_PCT_SUM_OPP", "HOME_MISSING_PTS_SUM_OPP"
        ]
    },
    {
        "name": "M092", "track": "100G (第三順位主力)", "threshold": 0.55,
        "features": [
            "HOME_LOOSE_BALLS_RECOVERED_S2D", "AWAY_CONTESTED_SHOTS_L10", "HOME_TS_PCT_L10", "HOME_PCT_AST_FGM_L5",
            "HOME_PACE_S2D", "HOME_PCT_PTS_3PT_L3", "AWAY_PCT_PTS_PAINT_L5", "HOME_CLUTCH_TS_PCT_S2D",
            "HOME_EFG_PCT_L10", "AWAY_SCREEN_ASSISTS_S2D", "HOME_TM_TOV_PCT_S2D", "AWAY_PCT_PTS_3PT_L10",
            "AWAY_RUNS_10_0_COUNT_L3", "HOME_RUNS_10_0_COUNT_S2D", "HOME_PACE_L10"
        ]
    },
    {
        "name": "M110", "track": "150G (第四順位重裝甲)", "threshold": 0.54,
        "features": [
            "AWAY_CONTESTED_SHOTS_L10", "HOME_PCT_AST_FGM_L5", "HOME_PACE_S2D", "HOME_PCT_PTS_PAINT_L5",
            "HOME_PACE_L10", "HOME_LOOSE_BALLS_RECOVERED_S2D", "AWAY_MID_FREQ_S2D", "AWAY_FTA_RATE_L3",
            "HOME_EFG_PCT_L10", "HOME_TM_TOV_PCT_S2D", "HOME_PCT_PTS_3PT_L5", "HOME_MOREYBALL_INDEX_L10",
            "AWAY_CHARGES_DRAWN_L10", "HOME_PCT_AST_FGM_L10", "HOME_MAX_UNANSWERED_RUN_L5", "HOME_PCT_PTS_3PT_L3",
            "AWAY_PCT_PTS_PAINT_L5", "HOME_DEF_RATING_L10", "AWAY_PACE_S2D", "HOME_CLUTCH_TS_PCT_S2D",
            "AWAY_TM_TOV_PCT_S2D", "AWAY_RUNS_10_0_COUNT_L3", "HOME_RUNS_10_0_COUNT_S2D", "HOME_TS_PCT_L10",
            "AWAY_SCREEN_ASSISTS_L5", "HOME_DEF_RATING_S2D", "AWAY_PCT_PTS_3PT_L10", "HOME_EFFICIENCY_TREND",
            "HOME_MISSING_PIE_SUM", "HOME_MISSING_USG_PCT_SUM_OPP"
        ]
    },
    {
        "name": "M126", "track": "200G (第五順位重砲)", "threshold": 0.53,
        "features": [
            "HOME_LOOSE_BALLS_RECOVERED_S2D", "AWAY_CONTESTED_SHOTS_L10", "HOME_TS_PCT_L10", "HOME_PCT_AST_FGM_L5",
            "HOME_PACE_S2D", "HOME_PCT_PTS_3PT_L3", "AWAY_PCT_PTS_PAINT_L5", "HOME_CLUTCH_TS_PCT_S2D",
            "HOME_EFG_PCT_L10", "AWAY_SCREEN_ASSISTS_S2D", "HOME_TM_TOV_PCT_S2D", "AWAY_PCT_PTS_3PT_L10",
            "AWAY_RUNS_10_0_COUNT_L3", "HOME_RUNS_10_0_COUNT_S2D", "HOME_PACE_L10", "HOME_MISSING_EFF_SUM"
        ]
    },
    {
        "name": "M014", "track": "Overall (最終防線)", "threshold": 0.00, # 最終防線不設門檻，直接給結果
        "features": [
            "HOME_DEF_RATING_L5", "HOME_Q1_Q3_GAP_L5", "HOME_RUN_DEFICIT_RECOVERY_RATE_L5", "HOME_REST_DAYS",
            "HOME_Q1_Q3_GAP_L10", "HOME_PACE_L10", "HOME_Q1_Q3_GAP_S2D", "HOME_CLUTCH_TS_PCT_L3",
            "AWAY_MID_FREQ_L10", "AWAY_IS_B2B", "AWAY_CHARGES_DRAWN_L5", "AWAY_Q1_Q3_GAP_L3",
            "HOME_PCT_PTS_PAINT_L5", "AWAY_TS_PCT_L10", "HOME_PCT_AST_FGM_S2D", "AWAY_DEF_RATING_L3",
            "HOME_PCT_PTS_3PT_L3", "HOME_RUNS_10_0_COUNT_L3", "AWAY_EFFICIENCY_TREND"
        ]
    }
]

def load_latest_features():
    """從歷史大表抓出各隊『最新』的滾動特徵狀態"""
    print("🔍 正在從特徵大表提取各隊最新實力指標...")
    df_master = pd.read_csv(MASTER_FEATURES_CSV, low_memory=False)
    
    # 取代 NaN，防呆保護
    df_master = df_master.fillna(0)
    
    # 因為 df_master 是按照時間順序生成的，我們只要取每支球隊「最後一次出現」的 row 即可
    latest_home = df_master.drop_duplicates(subset=['home_team'], keep='last').copy()
    latest_away = df_master.drop_duplicates(subset=['away_team'], keep='last').copy()
    
    # 建立字典：{ 'LAL': {HOME_PACE_L10: 102.3, ...}, ... }
    team_latest_home_stats = latest_home.set_index('home_team').to_dict('index')
    team_latest_away_stats = latest_away.set_index('away_team').to_dict('index')
    
    return team_latest_home_stats, team_latest_away_stats

def predict_upcoming_games():
    print("🚀 啟動 NBA 絕對王者瀑布流預測系統！")
    
    if not os.path.exists(UPCOMING_CSV):
        print("❌ 找不到今日賽程 (upcoming_games.csv)！今日可能無賽事。")
        return
        
    upcoming_df = pd.read_csv(UPCOMING_CSV)
    if upcoming_df.empty:
        print("🤷‍♂️ 今日無賽事需要預測。")
        return
        
    home_stats_dict, away_stats_dict = load_latest_features()
    
    # 載入模型
    models = {}
    for stage in WATERFALL_MODELS:
        m_name = stage['name']
        model_path = os.path.join(MODEL_DIR, f"{m_name}.pkl")
        if os.path.exists(model_path):
            models[m_name] = joblib.load(model_path)
        else:
            print(f"⚠️ 警告: 找不到模型檔案 {model_path}，請確認 train_deploy.py 已執行並產出模型。")
    
    if not models:
        print("❌ 沒有任何可用的模型，預測中止。")
        return

    predictions_log = []
    
    print(f"\n🎯 今日共有 {len(upcoming_df)} 場賽事，開始進行 AI 瀑布流分析...\n" + "="*60)
    
    for _, row in upcoming_df.iterrows():
        home_team = row['home_team']
        away_team = row['away_team']
        game_date = row['game_date']
        
        # 1. 組裝今日特徵向量
        home_features = home_stats_dict.get(home_team, {})
        away_features = away_stats_dict.get(away_team, {})
        
        # 將盤口資訊與 B2B 等即時資訊覆蓋進去
        today_context = {
            "HOME_IS_B2B": 1 if row.get('home_is_b2b', False) else 0,
            "AWAY_IS_B2B": 1 if row.get('away_is_b2b', False) else 0,
            # 若有其他即時計算的傷兵 PIE 可以加在這裡
        }
        
        # 2. 瀑布流決策引擎
        final_decision = None
        for stage in WATERFALL_MODELS:
            m_name = stage['name']
            if m_name not in models: continue
            
            # 準備該模型專屬的 X_test
            X_input = {}
            for feat in stage['features']:
                if feat in today_context:
                    X_input[feat] = today_context[feat]
                elif feat.startswith('HOME_'):
                    X_input[feat] = home_features.get(feat, 0)
                elif feat.startswith('AWAY_'):
                    X_input[feat] = away_features.get(feat, 0)
                else:
                    X_input[feat] = 0 # 缺值防呆
            
            X_df = pd.DataFrame([X_input])
            
            # 取得預測機率 (假設 1 為主隊勝)
            prob = models[m_name].predict_proba(X_df)[0]
            home_win_prob = prob[1]
            
            # 信心水準判斷
            confidence = max(prob[0], prob[1])
            predicted_winner = home_team if home_win_prob >= 0.5 else away_team
            
            if confidence >= stage['threshold'] or m_name == "M014":
                final_decision = {
                    "Game_Date": game_date,
                    "Matchup": f"{away_team} @ {home_team}",
                    "Predicted_Winner": predicted_winner,
                    "Confidence": round(confidence * 100, 2),
                    "Model_Used": m_name,
                    "Track_Name": stage['track']
                }
                break # 成功觸發，跳出瀑布流
                
        if final_decision:
            predictions_log.append(final_decision)
            print(f"🏀 {final_decision['Matchup']}")
            print(f"   👉 預測勝方: 【{final_decision['Predicted_Winner']}】")
            print(f"   📊 信心水準: {final_decision['Confidence']}% (由 {final_decision['Model_Used']} {final_decision['Track_Name']} 觸發)")
            print("-" * 60)

    # 輸出報表
    output_df = pd.DataFrame(predictions_log)
    os.makedirs(os.path.dirname(OUTPUT_PREDICTION), exist_ok=True)
    output_df.to_csv(OUTPUT_PREDICTION, index=False, encoding='utf-8-sig')
    print(f"\n✅ 今日預測報表已順利匯出至: {OUTPUT_PREDICTION}")

if __name__ == "__main__":
    predict_upcoming_games()