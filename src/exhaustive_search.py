import pandas as pd
import itertools
import time
import os
from catboost import CatBoostRegressor
from tqdm import tqdm

# 從我們寫好的共用模組直接載入完整數據！
from nba_daily_backtest import load_prepared_data

# ==========================================
# ⚙️ 窮舉測試設定區
# ==========================================
OUTPUT_FILE = "exhaustive_search_results_8to11.csv"
TEST_SEASON = '2025-26'
CONFIDENCE_THRESHOLD = 0.5

# 永遠必帶的基礎特徵 (球隊偏差)
BASE_FEATURES = ['home_team', 'away_team']

# 📦 將特徵「模組化」，避免 200 萬次的組合爆炸
FEATURE_BLOCKS = {
    "Elo戰力": ['home_elo', 'away_elo', 'elo_diff'],
    "R40_攻防": ['home_R40_OFF_RATING', 'home_R40_DEF_RATING', 'away_R40_OFF_RATING', 'away_R40_DEF_RATING'],
    "R5_攻防": ['home_R5_OFF_RATING', 'home_R5_DEF_RATING', 'away_R5_OFF_RATING', 'away_R5_DEF_RATING'],
    "R20_四因子": ['home_R20_FTA_RATE', 'away_R20_FTA_RATE', 'home_R20_TOV_PCT', 'away_R20_TOV_PCT', 'home_R20_OREB_PCT', 'away_R20_OREB_PCT'],
    "R10_四因子": ['home_R10_FTA_RATE', 'away_R10_FTA_RATE', 'home_R10_TOV_PCT', 'away_R10_TOV_PCT', 'home_R10_OREB_PCT', 'away_R10_OREB_PCT'],
    "R20_節奏": ['home_R20_PACE', 'away_R20_PACE'],
    "R10_節奏": ['home_R10_PACE', 'away_R10_PACE'],
    "R5_節奏": ['home_R5_PACE', 'away_R5_PACE'],
    "R40_攻防差值": ['diff_R40_OFF_DEF', 'diff_R40_DEF_OFF', 'diff_R40_PACE'],
    "傷病_NetRating": ['diff_missing_NET_RATING_r20', 'diff_active_rust_adj_NET_RATING'],
    "傷病_PIE": ['diff_missing_PIE_r20', 'diff_active_rust_adj_PIE']
}

def run_exhaustive_search():
    print("🚀 [MLOps] 啟動雲端暴力窮舉特徵測試 (Test on 2025-26 Season)")
    df = load_prepared_data()
    
    if df is None or df.empty:
        print("❌ 無法取得數據。")
        return

    # 1. 為了公平比較，先把包含 NaN 的列刪除 (確保所有模型測的比賽場次一模一樣)
    all_possible_features = [f for block in FEATURE_BLOCKS.values() for f in block]
    check_cols = BASE_FEATURES + all_possible_features + ['target_residual', 'vegas_line_h', 'real_diff']
    # 確保不會因為缺特徵而報錯
    valid_cols = [c for c in check_cols if c in df.columns]
    
    df_clean = df.dropna(subset=valid_cols).copy()
    
    # 2. 劃分訓練集 (歷史) 與 測試集 (最新賽季)
    train_df = df_clean[df_clean['season'] != TEST_SEASON]
    test_df = df_clean[df_clean['season'] == TEST_SEASON]
    
    print(f"📊 基準資料清洗完成！訓練集: {len(train_df)} 場 | 測試集: {len(test_df)} 場")
    
    # 3. 產生 8 到 11 個模組的所有組合 (進階大特徵池)
    block_names = list(FEATURE_BLOCKS.keys())
    all_combinations = []
    for r in range(8, 12):  # 組合長度 8 ~ 11
        all_combinations.extend(list(itertools.combinations(block_names, r)))

    # 4. 開始暴力窮舉迴圈
    for idx, combo in enumerate(tqdm(all_combinations, desc="訓練模型中")):
        # 展開這個組合底下的所有實際特徵名稱
        current_features = BASE_FEATURES.copy()
        for block_name in combo:
            current_features.extend(FEATURE_BLOCKS[block_name])
            
        # 建立輕量化快速模型 (迭代200次足夠看出特徵好壞，節省雲端時間)
        model = CatBoostRegressor(
            iterations=200, 
            learning_rate=0.05, 
            depth=6, 
            loss_function='RMSE', 
            verbose=False,
            cat_features=BASE_FEATURES,
            random_seed=42 # 固定種子，結果才公平
        )
        
        # 訓練
        model.fit(train_df[current_features], train_df['target_residual'])
        
        # 預測 2025-26 賽季
        preds = model.predict(test_df[current_features])
        
        # 結算成績
        bets_count = 0
        wins = 0
        
        for i, (index, row) in enumerate(test_df.iterrows()):
            pred_residual = preds[i]
            real_diff = row['real_diff']
            vegas_line = row['vegas_line_h']
            
            if pred_residual > CONFIDENCE_THRESHOLD:
                pick = 'Home'
            elif pred_residual < -CONFIDENCE_THRESHOLD:
                pick = 'Away'
            else:
                continue # Pass
                
            bets_count += 1
            home_covered = real_diff > vegas_line
            
            if pick == 'Home' and home_covered:
                wins += 1
            elif pick == 'Away' and not home_covered:
                wins += 1
                
        # 計算 ROI
        if bets_count > 0:
            win_pct = wins / bets_count
            roi = (win_pct * 0.9) - (1 - win_pct)
        else:
            win_pct = 0
            roi = 0
            
        # 紀錄結果
        combo_str = " + ".join(combo)
        results.append({
            "Combo_ID": idx + 1,
            "Blocks_Count": len(combo),
            "Feature_Count": len(current_features),
            "Combo_Name": combo_str,
            "Bets_Count": bets_count,
            "Win_Pct": round(win_pct * 100, 2),
            "ROI": round(roi * 100, 2)
        })
        
        # 每跑 500 個組合自動存檔一次，避免中斷全毀
        if (idx + 1) % 500 == 0:
            temp_df = pd.DataFrame(results).sort_values(by=['ROI', 'Win_Pct'], ascending=False)
            temp_df.to_csv(OUTPUT_FILE, index=False)

    # 5. 最終結算並排序輸出
    final_df = pd.DataFrame(results)
    final_df = final_df.sort_values(by=['ROI', 'Win_Pct'], ascending=False)
    final_df.to_csv(OUTPUT_FILE, index=False)
    
    elapsed = (time.time() - start_time) / 60
    print(f"\n✅ 窮舉測試完畢！總耗時: {elapsed:.1f} 分鐘")
    print(f"🏆 排行榜已儲存至 {OUTPUT_FILE}，你可以到 GitHub 上下載查看了！")
    
    # 印出前五名給你看
    print("\n👑 預測 ROI 前五名組合：")
    print(final_df.head(5)[['Combo_Name', 'Bets_Count', 'Win_Pct', 'ROI']].to_string(index=False))

if __name__ == "__main__":
    run_exhaustive_search()
