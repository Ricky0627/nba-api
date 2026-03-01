import pandas as pd
import numpy as np
import time
import os
from catboost import CatBoostRegressor
from tqdm import tqdm

# 從共用模組載入數據 (不影響原系統)
from nba_daily_backtest import load_prepared_data

OUTPUT_FILE = "v2_daily_backtest_results.csv"
TEST_SEASON = '2025-26'
CONFIDENCE_THRESHOLD = 0.5
BASE_FEATURES = ['home_team', 'away_team']

# ==========================================
# 1. 定義 V2 煉丹爐淬鍊出的最強三巨頭
# ==========================================
TOP_3_MODELS = [
    {
        "Name": "V2_Rank1_NoFantasy",
        "Features_List": [
            'home_R40_OFF_RATING', 'home_R40_DEF_RATING', 'away_R40_OFF_RATING', 'away_R40_DEF_RATING',
            'home_R10_FTA_RATE', 'away_R10_FTA_RATE', 'home_R10_TOV_PCT', 'away_R10_TOV_PCT', 'home_R10_OREB_PCT', 'away_R10_OREB_PCT',
            'home_R10_PACE', 'away_R10_PACE', 'diff_R40_OFF_DEF', 'diff_R40_DEF_OFF', 'diff_R40_PACE',
            'diff_missing_NET_RATING_r20', 'diff_active_rust_adj_NET_RATING', 'diff_missing_PIE_r20', 'diff_active_rust_adj_PIE'
        ]
    },
    {
        "Name": "V2_Rank2_Fantasy_Core",
        "Features_List": [
            'home_R20_FTA_RATE', 'away_R20_FTA_RATE', 'home_R20_TOV_PCT', 'away_R20_TOV_PCT', 'home_R20_OREB_PCT', 'away_R20_OREB_PCT',
            'home_R10_PACE', 'away_R10_PACE',
            'diff_missing_NET_RATING_r20', 'diff_active_rust_adj_NET_RATING', 'diff_missing_PIE_r20', 'diff_active_rust_adj_PIE',
            'diff_missing_NBA_FANTASY_PTS_r20', 'diff_active_rust_adj_NBA_FANTASY_PTS'
        ]
    },
    {
        "Name": "V2_Rank3_Fantasy_Pro",
        "Features_List": [
            'home_elo', 'away_elo', 'elo_diff',
            'home_R40_OFF_RATING', 'home_R40_DEF_RATING', 'away_R40_OFF_RATING', 'away_R40_DEF_RATING',
            'home_R10_FTA_RATE', 'away_R10_FTA_RATE', 'home_R10_TOV_PCT', 'away_R10_TOV_PCT', 'home_R10_OREB_PCT', 'away_R10_OREB_PCT',
            'home_R20_PACE', 'away_R20_PACE', 'home_R10_PACE', 'away_R10_PACE', 'home_R5_PACE', 'away_R5_PACE',
            'diff_R40_OFF_DEF', 'diff_R40_DEF_OFF', 'diff_R40_PACE',
            'diff_missing_NET_RATING_r20', 'diff_active_rust_adj_NET_RATING', 'diff_missing_PIE_r20', 'diff_active_rust_adj_PIE',
            'diff_missing_NBA_FANTASY_PTS_r20', 'diff_active_rust_adj_NBA_FANTASY_PTS'
        ]
    }
]

# 合併基礎特徵
for m in TOP_3_MODELS:
    m["Features"] = BASE_FEATURES + m["Features_List"]

# ==========================================
# 2. 攔截並覆寫：全新進階 Elo 演算法
# ==========================================
def apply_advanced_elo(df):
    print("✨ [V2 回測] 正在套用進階 Elo 演算法 (跨賽季均值回歸 & 勝分差乘數)...")
    df = df.sort_values(['date', 'game_id']).copy()
    
    HOME_ADV_ELO = 100
    ELO_K = 20
    
    elo_dict = {team: 1500.0 for team in set(df['home_team']) | set(df['away_team'])}
    home_elos, away_elos = [], []
    current_season = None
    
    for _, row in df.iterrows():
        if current_season != row['season']:
            if current_season is not None:
                for team in elo_dict:
                    elo_dict[team] = (elo_dict[team] * 0.75) + (1505 * 0.25)
            current_season = row['season']
            
        h, a = row['home_team'], row['away_team']
        home_elos.append(elo_dict[h])
        away_elos.append(elo_dict[a])
        
        if pd.notnull(row.get('real_diff')):
            prob_h = 1 / (1 + 10 ** ((elo_dict[a] - (elo_dict[h] + HOME_ADV_ELO)) / 400))
            actual_h = 1.0 if row['real_diff'] > 0 else 0.0
            
            mov = abs(row['real_diff'])
            mov_multiplier = np.log(mov + 1) if mov > 0 else 1.0
            
            elo_shift = ELO_K * mov_multiplier * (actual_h - prob_h)
            elo_dict[h] += elo_shift
            elo_dict[a] -= elo_shift
            
    df['home_elo'] = home_elos
    df['away_elo'] = away_elos
    df['elo_diff'] = df['home_elo'] + HOME_ADV_ELO - df['away_elo']
    return df

# ==========================================
# 3. 執行逐日滾動回測
# ==========================================
def run_v2_daily_backtest():
    print("🚀 [MLOps] 啟動 V2 模型逐日滾動回測 (搭載最佳超參數)")
    df_raw = load_prepared_data()
    
    if df_raw is None or df_raw.empty:
        print("❌ 無法取得數據。")
        return
        
    df = apply_advanced_elo(df_raw)
    df = df.dropna(subset=['date']).sort_values(['date', 'game_id'])

    test_games = df[df['season'] == TEST_SEASON].copy()
    unique_dates = sorted(test_games['date'].unique())
    
    print(f"📅 準備對 2025-26 賽季的 {len(unique_dates)} 個比賽日進行「逐日推進」回測...")

    model_stats = {m["Name"]: {"Total_Games": 0, "Total_Correct": 0, "Bets_Count": 0, "Bets_Won": 0} for m in TOP_3_MODELS}
    start_time = time.time()

    for current_date in tqdm(unique_dates, desc="📆 逐日推進中"):
        historical_data = df[df['date'] < current_date]
        todays_games = df[df['date'] == current_date]
        
        if todays_games.empty:
            continue
            
        for m in TOP_3_MODELS:
            features = m["Features"]
            
            curr_train = historical_data.dropna(subset=features + ['target_residual'])
            curr_test = todays_games.dropna(subset=features + ['target_residual'])
            
            if curr_test.empty:
                continue
                
            # 🔥 搭載煉丹爐淬鍊出的最佳火候！
            model = CatBoostRegressor(
                iterations=300, 
                learning_rate=0.1, 
                depth=8, 
                l2_leaf_reg=1, 
                subsample=0.9, 
                loss_function='RMSE', 
                verbose=False, 
                cat_features=BASE_FEATURES,
                random_seed=42
            )
            
            model.fit(curr_train[features], curr_train['target_residual'])
            preds = model.predict(curr_test[features])
            
            for idx, (game_idx, row) in enumerate(curr_test.iterrows()):
                pred_res = preds[idx]
                actual_res = row['target_residual'] 
                
                # 總勝率 (方向正確即算對)
                if (pred_res > 0 and actual_res > 0) or (pred_res < 0 and actual_res < 0):
                    model_stats[m["Name"]]["Total_Correct"] += 1
                model_stats[m["Name"]]["Total_Games"] += 1
                
                # 下注勝率 (超過信心門檻)
                if abs(pred_res) > CONFIDENCE_THRESHOLD:
                    model_stats[m["Name"]]["Bets_Count"] += 1
                    if (pred_res > 0 and actual_res > 0) or (pred_res < 0 and actual_res < 0):
                        model_stats[m["Name"]]["Bets_Won"] += 1

    # ==========================================
    # 📊 產出報告
    # ==========================================
    results_list = []
    for m in TOP_3_MODELS:
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
    print(f"\n✅ V2 逐日回測完畢！總耗時: {elapsed:.1f} 分鐘")
    print(f"🏆 V2 旗艦模型實戰排行榜已儲存至 {OUTPUT_FILE}！\n")
    print(report_df.to_string(index=False))

if __name__ == "__main__":
    run_v2_daily_backtest()
