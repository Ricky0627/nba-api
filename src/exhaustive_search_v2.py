import pandas as pd
import numpy as np
import itertools
import time
import os
import traceback
from catboost import CatBoostRegressor
from tqdm import tqdm

# 從原本的模組載入數據，完全不動到原本的檔案
from nba_daily_backtest import load_prepared_data

# ==========================================
# ⚙️ V2 窮舉實驗設定區
# ==========================================
OUTPUT_FILE = "exhaustive_search_v2_results.csv"
TEST_SEASON = '2025-26'
CONFIDENCE_THRESHOLD = 0.5
BASE_FEATURES = ['home_team', 'away_team']

# 📦 解鎖第 12 塊拼圖：Fantasy Points
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
    "傷病_PIE": ['diff_missing_PIE_r20', 'diff_active_rust_adj_PIE'],
    "傷病_Fantasy": ['diff_missing_NBA_FANTASY_PTS_r20', 'diff_active_rust_adj_NBA_FANTASY_PTS'] # 🔥 新增 FP！
}

# ==========================================
# 🔧 攔截並覆寫：全新進階 Elo 演算法
# ==========================================
def apply_advanced_elo(df):
    print("✨ [V2 實驗] 正在覆寫 Elo 欄位 (加入跨賽季回歸 & 勝分差 MOV 乘數)...")
    df = df.sort_values(['date', 'game_id']).copy()
    
    HOME_ADV_ELO = 100
    ELO_K = 20
    
    elo_dict = {team: 1500.0 for team in set(df['home_team']) | set(df['away_team'])}
    home_elos, away_elos = [], []
    current_season = None
    
    for _, row in df.iterrows():
        # 🔥 優化 1：跨賽季回歸均值 (Mean Reversion)
        if current_season != row['season']:
            if current_season is not None:
                for team in elo_dict:
                    elo_dict[team] = (elo_dict[team] * 0.75) + (1505 * 0.25)
            current_season = row['season']
            
        h, a = row['home_team'], row['away_team']
        home_elos.append(elo_dict[h])
        away_elos.append(elo_dict[a])
        
        # 🔥 優化 2：勝分差乘數 (Margin of Victory Multiplier)
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
# 🚀 執行窮舉搜尋
# ==========================================
def run_exhaustive_search_v2():
    try:
        print("🚀 [MLOps] 啟動 V2 特徵實驗室 (進階 Elo + Fantasy Points)")
        # 1. 讀取原始數據
        df_raw = load_prepared_data()
        if df_raw is None or df_raw.empty:
            print("❌ 無法取得數據。")
            return
            
        # 2. 攔截並套用進階 Elo
        df = apply_advanced_elo(df_raw)

        # 3. 清洗資料
        all_possible_features = [f for block in FEATURE_BLOCKS.values() for f in block]
        check_cols = BASE_FEATURES + all_possible_features + ['target_residual', 'vegas_line_h', 'real_diff']
        valid_cols = [c for c in check_cols if c in df.columns]
        
        df_clean = df.dropna(subset=valid_cols).copy()
        
        train_df = df_clean[df_clean['season'] != TEST_SEASON]
        test_df = df_clean[df_clean['season'] == TEST_SEASON]
        
        print(f"📊 基準資料清洗完成！訓練集: {len(train_df)} 場 | 測試集: {len(test_df)} 場")
        
        # 4. 產生 5 到 12 個模組的所有組合
        block_names = list(FEATURE_BLOCKS.keys())
        all_combinations = []
        for r in range(5, 13):  # 長度 5 ~ 12
            all_combinations.extend(list(itertools.combinations(block_names, r)))
            
        print(f"🔥 即將測試 {len(all_combinations)} 種特徵組合 (預估約 40 分鐘)...")
        
        results = []
        start_time = time.time()

        # 5. 開始窮舉
        for idx, combo in enumerate(tqdm(all_combinations, desc="訓練模型中")):
            current_features = BASE_FEATURES.copy()
            for block_name in combo:
                current_features.extend(FEATURE_BLOCKS[block_name])
                
            current_features = [f for f in current_features if f in train_df.columns]
                
            model = CatBoostRegressor(
                iterations=200, learning_rate=0.05, depth=6, 
                loss_function='RMSE', verbose=False,
                cat_features=BASE_FEATURES, random_seed=42 
            )
            
            model.fit(train_df[current_features], train_df['target_residual'])
            preds = model.predict(test_df[current_features])
            
            bets_count, wins = 0, 0
            
            for i, (index, row) in enumerate(test_df.iterrows()):
                pred_residual = preds[i]
                real_diff = row['real_diff']
                vegas_line = row['vegas_line_h']
                
                if pred_residual > CONFIDENCE_THRESHOLD: pick = 'Home'
                elif pred_residual < -CONFIDENCE_THRESHOLD: pick = 'Away'
                else: continue 
                    
                bets_count += 1
                home_covered = real_diff > vegas_line
                
                if pick == 'Home' and home_covered: wins += 1
                elif pick == 'Away' and not home_covered: wins += 1
                    
            if bets_count > 0:
                win_pct = wins / bets_count
                roi = (win_pct * 0.9) - (1 - win_pct)
            else:
                win_pct, roi = 0, 0
                
            results.append({
                "Combo_ID": idx + 1, "Blocks_Count": len(combo), "Feature_Count": len(current_features),
                "Combo_Name": " + ".join(combo), "Bets_Count": bets_count,
                "Win_Pct": round(win_pct * 100, 2), "ROI": round(roi * 100, 2)
            })
            
            if (idx + 1) % 200 == 0:
                pd.DataFrame(results).sort_values(by=['ROI', 'Win_Pct'], ascending=False).to_csv(OUTPUT_FILE, index=False)

        # 6. 最終結算
        final_df = pd.DataFrame(results).sort_values(by=['ROI', 'Win_Pct'], ascending=False)
        final_df.to_csv(OUTPUT_FILE, index=False)
        
        elapsed = (time.time() - start_time) / 60
        print(f"\n✅ V2 窮舉測試完畢！總耗時: {elapsed:.1f} 分鐘")
        print(f"🏆 排行榜已儲存至 {OUTPUT_FILE}！")
        print("\n👑 預測 ROI 前五名組合：")
        print(final_df.head(5)[['Combo_Name', 'Bets_Count', 'Win_Pct', 'ROI']].to_string(index=False))

    except Exception as e:
        print("\n❌ 發生致命錯誤：")
        traceback.print_exc()

if __name__ == "__main__":
    run_exhaustive_search_v2()
