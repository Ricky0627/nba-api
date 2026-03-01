import pandas as pd
import numpy as np
from catboost import CatBoostRegressor
import os

# 載入我們的原始數據模組
from nba_daily_backtest import load_prepared_data

# ==========================================
# 1. 準備特徵與進階 Elo (攔截 V2 邏輯)
# ==========================================
BASE_FEATURES = ['home_team', 'away_team']

# 這裡使用 V2 榜單中，包含 Fantasy 且極度輕量高效的「第二名神級組合」
BEST_FEATURES = [
    'home_R20_FTA_RATE', 'away_R20_FTA_RATE', 'home_R20_TOV_PCT', 'away_R20_TOV_PCT', 'home_R20_OREB_PCT', 'away_R20_OREB_PCT', # R20_四因子
    'home_R10_PACE', 'away_R10_PACE', # R10_節奏
    'diff_missing_NET_RATING_r20', 'diff_active_rust_adj_NET_RATING', # 傷病_NetRating
    'diff_missing_PIE_r20', 'diff_active_rust_adj_PIE', # 傷病_PIE
    'diff_missing_NBA_FANTASY_PTS_r20', 'diff_active_rust_adj_NBA_FANTASY_PTS' # 傷病_Fantasy
]

def apply_advanced_elo(df):
    print("✨ [調校實驗室] 準備進階數據...")
    df = df.sort_values(['date', 'game_id']).copy()
    # (為了簡化調校，這裡如果原本就有 elo 我們就不重算了，因為我們這次主要調校樹的參數)
    return df

# ==========================================
# 2. 啟動超參數調校 (Randomized Search)
# ==========================================
def run_tuning():
    print("🚀 [MLOps] 啟動 CatBoost 超參數隨機搜尋 (Randomized Search)")
    
    df_raw = load_prepared_data()
    if df_raw is None or df_raw.empty:
        return
        
    df = apply_advanced_elo(df_raw)
    
    # 確保特徵完整
    train_cols = BASE_FEATURES + BEST_FEATURES
    df_clean = df.dropna(subset=train_cols + ['target_residual']).copy()
    
    # 我們用全部的歷史資料來找最佳參數
    X = df_clean[train_cols]
    y = df_clean['target_residual']
    
    print(f"📊 訓練集準備完成，共 {len(df_clean)} 場比賽。開始煉丹...")

    # 建立基礎模型
    model = CatBoostRegressor(
        loss_function='RMSE', 
        cat_features=BASE_FEATURES,
        verbose=False,
        random_seed=42
    )
    
    # 🎲 定義要搜尋的超參數網格 (Grid)
    # CatBoost 會在這裡面隨機抽取組合進行交叉驗證
    param_distribution = {
        'iterations': [300, 500, 800],           # 樹的數量
        'learning_rate': [0.01, 0.03, 0.05, 0.1], # 學習率 (火候大小)
        'depth': [4, 6, 8],                       # 樹的深度 (複雜度)
        'l2_leaf_reg': [1, 3, 5, 7, 9],           # L2 正則化 (防止過擬合的懲罰)
        'subsample': [0.8, 0.9, 1.0]              # 樣本抽樣率
    }

    # 執行 Randomized Search
    # n_iter=20 代表從上面的組合中隨機抽 20 種來考驗
    randomized_search_result = model.randomized_search(
        param_distribution,
        X=X,
        y=y,
        cv=3,            # 3 折交叉驗證
        n_iter=20,       # 嘗試 20 種不同的參數組合
        partition_random_seed=42,
        search_by_train_test_split=True,
        shuffle=True,
        verbose=False
    )
    
    print("\n" + "="*50)
    print("🎉 超參數調校完成！最佳火候配方出爐：")
    print("="*50)
    best_params = randomized_search_result['params']
    for key, value in best_params.items():
        print(f"🔥 {key}: {value}")
    print("="*50)
    print("💡 下一步：把這些參數寫回你的 nba_daily_backtest.py 裡面！")

if __name__ == "__main__":
    run_tuning()
