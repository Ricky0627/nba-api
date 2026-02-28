import pandas as pd
import numpy as np
import time
import os
from catboost import CatBoostRegressor
from tqdm import tqdm

# 從共用模組直接載入完整數據
from nba_daily_backtest import load_prepared_data

OUTPUT_FILE = "top10_daily_backtest_results.csv"
TEST_SEASON = '2025-26'
CONFIDENCE_THRESHOLD = 0.5

# ==========================================
# ⚙️ 模組定義與前十強陣容
# ==========================================
BASE_FEATURES = ['home_team', 'away_team']

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

# 挑選出的 10 組最強組合
TOP_10_COMBOS = [
    # 來自 1~7 模組的前 5 名
    {"Name": "7B_Rank1", "Blocks": ["R5_攻防", "R10_四因子", "R10_節奏", "R5_節奏", "傷病_NetRating", "傷病_PIE"]},
    {"Name": "7B_Rank2", "Blocks": ["R20_四因子", "R10_四因子", "R10_節奏", "R5_節奏", "傷病_NetRating", "傷病_PIE"]},
    {"Name": "7B_Rank3", "Blocks": ["Elo戰力", "R10_四因子", "R10_節奏", "R5_節奏", "傷病_PIE"]},
    {"Name": "7B_Rank4", "Blocks": ["R20_四因子", "R5_節奏", "傷病_NetRating", "傷病_PIE"]},
    {"Name": "7B_Rank5", "Blocks": ["Elo戰力", "R40_攻防", "R20_四因子", "R5_節奏", "傷病_NetRating", "傷病_PIE"]},
    # 來自 8~11 模組的前 5 名
    {"Name": "11B_Rank1", "Blocks": ["R5_攻防", "R20_四因子", "R10_四因子", "R20_節奏", "R10_節奏", "R5_節奏", "傷病_NetRating", "傷病_PIE"]},
    {"Name": "11B_Rank2", "Blocks": ["R40_攻防", "R5_攻防", "R20_四因子", "R10_四因子", "R10_節奏", "R40_攻防差值", "傷病_NetRating", "傷病_PIE"]},
    {"Name": "11B_Rank3", "Blocks": ["Elo戰力", "R40_攻防", "R5_攻防", "R20_四因子", "R10_四因子", "R5_節奏", "R40_攻防差值", "傷病_NetRating"]},
    {"Name": "11B_Rank4", "Blocks": ["R40_攻防", "R5_攻防", "R20_四因子", "R10_四因子", "R10_節奏", "R5_節奏", "R40_攻防差值", "傷病_NetRating", "傷病_PIE"]},
    {"Name": "11B_Rank5", "Blocks": ["Elo戰力", "R40_攻防", "R5_攻防", "R20_四因子", "R20_節奏", "R5_節奏", "R40_攻防差值", "傷病_NetRating", "傷病_PIE"]},
]

# 將 Block 轉換為實際特徵
for combo in TOP_10_COMBOS:
    feats = BASE_FEATURES.copy()
    for b in combo["Blocks"]:
        feats.extend(FEATURE_BLOCKS[b])
    combo["Features"] = feats

# ==========================================
# 🚀 執行逐日回測
# ==========================================
def run_top10_daily_backtest():
    print("🚀 [MLOps] 啟動 10 大黃金組合：逐日滾動回測 (模擬真實下注)")
    df = load_prepared_data()
    
    if df is None or df.empty:
        print("❌ 無法取得數據。")
        return
        
    # 鎖定絕對排序，避免隨機性
    df = df.dropna(subset=['date']).sort_values(['date', 'game_id'])

    # 取得測試賽季的所有日期
    test_games = df[df['season'] == TEST_SEASON].copy()
    unique_dates = sorted(test_games['date'].unique())
    
    print(f"📅 準備對 2025-26 賽季的 {len(unique_dates)} 個比賽日進行「逐日推進」回測...")

    # 用來統計每個模型的成績
    model_stats = {m["Name"]: {"Total_Games": 0, "Total_Correct": 0, "Bets_Count": 0, "Bets_Won": 0} for m in TOP_10_COMBOS}

    start_time = time.time()

    # 模擬時光機，逐日推進
    for current_date in tqdm(unique_dates, desc="📆 逐日推進中"):
        historical_data = df[df['date'] < current_date]
        todays_games = df[df['date'] == current_date]
        
        if todays_games.empty:
            continue
            
        for m in TOP_10_COMBOS:
            features = m["Features"]
            
            # 過濾缺失值
            curr_train = historical_data.dropna(subset=features)
            curr_test = todays_games.dropna(subset=features)
            
            if curr_test.empty:
                continue
                
            # 建立模型 (迭代加到 500 次，符合實戰)
            model = CatBoostRegressor(
                iterations=500, 
                learning_rate=0.03, depth=6, 
                loss_function='RMSE', verbose=False, 
                cat_features=BASE_FEATURES,
                random_seed=42 # 鎖定隨機種子
            )
            
            # 訓練與預測
            model.fit(curr_train[features], curr_train['target_residual'])
            preds = model.predict(curr_test[features])
            
            # 結算今日成績
            for idx, (game_idx, row) in enumerate(curr_test.iterrows()):
                pred_res = preds[idx]
                actual_res = row['target_residual'] # 實際殘差 (正=主過盤, 負=客過盤)
                
                # 1. 總勝率邏輯 (只要預測方向與實際方向一致即算對)
                if (pred_res > 0 and actual_res > 0) or (pred_res < 0 and actual_res < 0):
                    model_stats[m["Name"]]["Total_Correct"] += 1
                model_stats[m["Name"]]["Total_Games"] += 1
                
                # 2. 下注勝率邏輯 (超過門檻才下注)
                if abs(pred_res) > CONFIDENCE_THRESHOLD:
                    model_stats[m["Name"]]["Bets_Count"] += 1
                    if (pred_res > 0 and actual_res > 0) or (pred_res < 0 and actual_res < 0):
                        model_stats[m["Name"]]["Bets_Won"] += 1

    # ==========================================
    # 📊 產出報告
    # ==========================================
    results_list = []
    for m in TOP_10_COMBOS:
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
    print(f"\n✅ 逐日回測完畢！總耗時: {elapsed:.1f} 分鐘")
    print(f"🏆 十大模型實戰排行榜已儲存至 {OUTPUT_FILE}！\n")
    print(report_df.to_string(index=False))

if __name__ == "__main__":
    run_top10_daily_backtest()
