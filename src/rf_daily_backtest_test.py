import pandas as pd
import numpy as np
import time
import os
from sklearn.ensemble import RandomForestRegressor
from tqdm import tqdm

# 從共用模組載入數據
from nba_daily_backtest import load_prepared_data

OUTPUT_FILE = "rf_daily_backtest_results.csv"
TEST_SEASON = '2025-26'
CONFIDENCE_THRESHOLD = 0.5

# ==========================================
# 1. 準備最強特徵 (完全拔除球隊名稱字串)
# ==========================================
# 這裡我們放上之前驗證過的最強 V1 組合，加上剛剛的超輕量 Fantasy 組合來讓 RF 比較
TOP_MODELS = [
    {
        # 當初測出 8.33% 的神級特徵
        "Name": "RF_Rank1_True_God",
        "Features": [
            'home_elo', 'away_elo', 'elo_diff',
            'home_R40_OFF_RATING', 'home_R40_DEF_RATING', 'away_R40_OFF_RATING', 'away_R40_DEF_RATING',
            'home_R20_FTA_RATE', 'away_R20_FTA_RATE', 'home_R20_TOV_PCT', 'away_R20_TOV_PCT', 'home_R20_OREB_PCT', 'away_R20_OREB_PCT',
            'home_R5_PACE', 'away_R5_PACE',
            'diff_missing_NET_RATING_r20', 'diff_active_rust_adj_NET_RATING',
            'diff_missing_PIE_r20', 'diff_active_rust_adj_PIE'
        ]
    },
    {
        # 超輕量核心版 (看看 RF 能不能發揮它的潛力)
        "Name": "RF_Fantasy_Core",
        "Features": [
            'home_R20_FTA_RATE', 'away_R20_FTA_RATE', 'home_R20_TOV_PCT', 'away_R20_TOV_PCT', 'home_R20_OREB_PCT', 'away_R20_OREB_PCT',
            'home_R10_PACE', 'away_R10_PACE',
            'diff_missing_NET_RATING_r20', 'diff_active_rust_adj_NET_RATING',
            'diff_missing_PIE_r20', 'diff_active_rust_adj_PIE',
            'diff_missing_NBA_FANTASY_PTS_r20', 'diff_active_rust_adj_NBA_FANTASY_PTS'
        ]
    }
]

# ==========================================
# 2. 執行 Random Forest 逐日回測
# ==========================================
def run_rf_daily_backtest():
    print("🚀 [MLOps] 啟動 Random Forest (隨機森林) 逐日滾動回測")
    df = load_prepared_data()
    
    if df is None or df.empty:
        print("❌ 無法取得數據。")
        return
        
    df = df.dropna(subset=['date']).sort_values(['date', 'game_id'])

    test_games = df[df['season'] == TEST_SEASON].copy()
    unique_dates = sorted(test_games['date'].unique())
    
    print(f"📅 準備對 2025-26 賽季的 {len(unique_dates)} 個比賽日進行 RF 逐日回測...")

    model_stats = {m["Name"]: {"Total_Games": 0, "Total_Correct": 0, "Bets_Count": 0, "Bets_Won": 0} for m in TOP_MODELS}
    start_time = time.time()

    for current_date in tqdm(unique_dates, desc="📆 RF 逐日推進中"):
        historical_data = df[df['date'] < current_date]
        todays_games = df[df['date'] == current_date]
        
        if todays_games.empty:
            continue
            
        for m in TOP_MODELS:
            features = m["Features"]
            
            # Scikit-Learn 的 RF 不吃 NaN，必須嚴格過濾
            curr_train = historical_data.dropna(subset=features + ['target_residual'])
            curr_test = todays_games.dropna(subset=features + ['target_residual'])
            
            if curr_test.empty:
                continue
                
            # 🌲 建立隨機森林回歸模型
            # n_estimators=200: 建立 200 棵樹來投票 (裝袋法抗雜訊)
            # max_depth=6: 限制樹的深度，防止過擬合
            # min_samples_leaf=4: 每個葉子節點至少 4 個樣本，進一步防止死背答案
            # n_jobs=-1: 雲端全核心火力全開運算
            model = RandomForestRegressor(
                n_estimators=200,
                max_depth=6,
                min_samples_leaf=4,
                random_state=42,
                n_jobs=-1
            )
            
            # 訓練與預測 (完全不放球隊名稱)
            model.fit(curr_train[features], curr_train['target_residual'])
            preds = model.predict(curr_test[features])
            
            for idx, (game_idx, row) in enumerate(curr_test.iterrows()):
                pred_res = preds[idx]
                actual_res = row['target_residual'] 
                
                if (pred_res > 0 and actual_res > 0) or (pred_res < 0 and actual_res < 0):
                    model_stats[m["Name"]]["Total_Correct"] += 1
                model_stats[m["Name"]]["Total_Games"] += 1
                
                if abs(pred_res) > CONFIDENCE_THRESHOLD:
                    model_stats[m["Name"]]["Bets_Count"] += 1
                    if (pred_res > 0 and actual_res > 0) or (pred_res < 0 and actual_res < 0):
                        model_stats[m["Name"]]["Bets_Won"] += 1

    # ==========================================
    # 📊 產出報告
    # ==========================================
    results_list = []
    for m in TOP_MODELS:
        stats = model_stats[m["Name"]]
        
        total_games = stats["Total_Games"]
        total_win_pct = (stats["Total_Correct"] / total_games) if total_games > 0 else 0
        
        bets_count = stats["Bets_Count"]
        bet_win_pct = (stats["Bets_Won"] / bets_count) if bets_count > 0 else 0
        roi = (bet_win_pct * 0.9) - (1 - bet_win_pct) if bets_count > 0 else 0
        
        results_list.append({
            "Model_Name": m["Name"],
            "Total_Games": total_games,
            "Total_Win_Pct": f"{total_win_pct*100:.2f}%",
            "Bets_Count": bets_count,
            "Bet_Win_Pct": f"{bet_win_pct*100:.2f}%",
            "ROI": f"{roi*100:.2f}%"
        })
        
    report_df = pd.DataFrame(results_list).sort_values(by="ROI", ascending=False)
    report_df.to_csv(OUTPUT_FILE, index=False)
    
    elapsed = (time.time() - start_time) / 60
    print(f"\n✅ Random Forest 逐日回測完畢！總耗時: {elapsed:.1f} 分鐘")
    print(f"🏆 RF 模型實戰排行榜已儲存至 {OUTPUT_FILE}！\n")
    print(report_df.to_string(index=False))

if __name__ == "__main__":
    run_rf_daily_backtest()
