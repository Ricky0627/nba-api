import os
import pandas as pd
from catboost import CatBoostRegressor

# 🔥 直接從我們剛剛完美升級的 backtest 模組中，借用「歷史與最新數據合體」的函數！
from nba_daily_backtest import load_prepared_data

# 1. 設定：我們最強的三個模型 (來自回測榜單)
TOP_MODELS = {
    "Inj_All_19": ['home_R40_OFF_RATING', 'home_R40_DEF_RATING', 'away_R40_OFF_RATING', 'away_R40_DEF_RATING', 'home_R5_OFF_RATING', 'home_R5_DEF_RATING', 'away_R5_OFF_RATING', 'away_R5_DEF_RATING', 'home_elo', 'away_elo', 'elo_diff', 'home_R10_FTA_RATE', 'away_R10_FTA_RATE', 'home_R10_TOV_PCT', 'away_R10_TOV_PCT', 'home_R10_OREB_PCT', 'away_R10_OREB_PCT', 'home_R20_FTA_RATE', 'away_R20_FTA_RATE', 'home_R20_TOV_PCT', 'away_R20_TOV_PCT', 'home_R20_OREB_PCT', 'away_R20_OREB_PCT', 'diff_missing_NET_RATING_r20', 'diff_active_rust_adj_NET_RATING', 'diff_missing_PIE_r20', 'diff_active_rust_adj_PIE', 'home_R5_PACE', 'away_R5_PACE'],
    
    "Inj_All_3": ['home_elo', 'away_elo', 'elo_diff', 'home_R10_FTA_RATE', 'away_R10_FTA_RATE', 'home_R10_TOV_PCT', 'away_R10_TOV_PCT', 'home_R10_OREB_PCT', 'away_R10_OREB_PCT', 'diff_missing_NET_RATING_r20', 'diff_active_rust_adj_NET_RATING', 'diff_missing_PIE_r20', 'diff_active_rust_adj_PIE', 'diff_R40_OFF_DEF', 'diff_R40_DEF_OFF', 'diff_R40_PACE', 'home_R10_PACE', 'away_R10_PACE', 'home_R20_PACE', 'away_R20_PACE', 'home_R5_PACE', 'away_R5_PACE'],
    
    "Inj_All_24": ['home_R40_OFF_RATING', 'home_R40_DEF_RATING', 'away_R40_OFF_RATING', 'away_R40_DEF_RATING', 'home_elo', 'away_elo', 'elo_diff', 'home_R40_FTA_RATE', 'away_R40_FTA_RATE', 'home_R40_TOV_PCT', 'away_R40_TOV_PCT', 'home_R40_OREB_PCT', 'away_R40_OREB_PCT', 'diff_missing_NET_RATING_r20', 'diff_active_rust_adj_NET_RATING', 'diff_missing_PIE_r20', 'diff_active_rust_adj_PIE', 'home_R10_PACE', 'away_R10_PACE']
}

MODEL_DIR = 'models'

def train_and_save():
    # 確保儲存模型的資料夾存在
    if not os.path.exists(MODEL_DIR):
        os.makedirs(MODEL_DIR)
        
    print("⏳ [MLOps] 正在呼叫共用模組準備訓練數據...")
    try:
        # 直接使用我們寫好的雲端合體大絕招
        df = load_prepared_data()
    except Exception as e:
        print(f"❌ 讀取資料失敗: {e}")
        return

    if df is None or df.empty:
        print("❌ 無法取得有效資料，中止訓練。")
        return

    print(f"\n📊 訓練資料集大小: {len(df)} 場比賽，準備進行 CatBoost 訓練！")
    
    for name, features in TOP_MODELS.items():
        print(f"🚀 正在訓練 {name} ...")
        
        # 確保所有需要的特徵都在大表裡面
        valid_features = [f for f in features if f in df.columns]
        if len(valid_features) != len(features):
            missing = set(features) - set(valid_features)
            print(f"   ⚠️ 警告: {name} 缺少特徵 {missing}，將只使用現有特徵訓練。")
        
        # 移除空值
        train_df = df.dropna(subset=valid_features + ['target_residual'])
        
        # 訓練模型 (用 800 次迭代讓它學好學滿)
        model = CatBoostRegressor(iterations=800, learning_rate=0.03, depth=6, verbose=False)
        model.fit(train_df[valid_features], train_df['target_residual'])
        
        # 儲存模型
        save_path = os.path.join(MODEL_DIR, f"{name}.cbm")
        model.save_model(save_path)
        print(f"   ✅ 模型 {name} 訓練完畢並已儲存至: {save_path}\n")

if __name__ == "__main__":
    print("========================================")
    print(" 🤖 啟動模型打包與部署系統 (Model Deploy)")
    print("========================================")
    train_and_save()