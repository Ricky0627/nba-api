import pandas as pd
import numpy as np
import time
import os
from catboost import CatBoostRegressor
from sklearn.model_selection import train_test_split
from tqdm import tqdm

# 🔥 引入雲端合體神模組
from prepare_data import get_merged_dataframe

# --- 設定參數 ---
INJURY_FEATURES_FILE = 'nba_advanced_injury_features.csv'
PREDICTIONS_FILE = "nba_daily_walkforward_predictions.csv"
SUMMARY_FILE = "nba_daily_walkforward_summary.csv"

TEST_SEASON = '2025-26'
CONFIDENCE_THRESHOLD = 0.5  # 殘差 > 0.5 才下注
ELO_K = 20
HOME_ADV_ELO = 100

# ==========================================
# 1. 準備最強的三巨頭模型
# ==========================================
def get_top_models():
    models = [
        {
            "Name": "Inj_All_19",
            "Features_List": ['home_R40_OFF_RATING', 'home_R40_DEF_RATING', 'away_R40_OFF_RATING', 'away_R40_DEF_RATING', 'home_R5_OFF_RATING', 'home_R5_DEF_RATING', 'away_R5_OFF_RATING', 'away_R5_DEF_RATING', 'home_elo', 'away_elo', 'elo_diff', 'home_R10_FTA_RATE', 'away_R10_FTA_RATE', 'home_R10_TOV_PCT', 'away_R10_TOV_PCT', 'home_R10_OREB_PCT', 'away_R10_OREB_PCT', 'home_R20_FTA_RATE', 'away_R20_FTA_RATE', 'home_R20_TOV_PCT', 'away_R20_TOV_PCT', 'home_R20_OREB_PCT', 'away_R20_OREB_PCT', 'diff_missing_NET_RATING_r20', 'diff_active_rust_adj_NET_RATING', 'diff_missing_PIE_r20', 'diff_active_rust_adj_PIE', 'home_R5_PACE', 'away_R5_PACE']
        },
        {
            "Name": "Inj_All_3",
            "Features_List": ['home_elo', 'away_elo', 'elo_diff', 'home_R10_FTA_RATE', 'away_R10_FTA_RATE', 'home_R10_TOV_PCT', 'away_R10_TOV_PCT', 'home_R10_OREB_PCT', 'away_R10_OREB_PCT', 'diff_missing_NET_RATING_r20', 'diff_active_rust_adj_NET_RATING', 'diff_missing_PIE_r20', 'diff_active_rust_adj_PIE', 'diff_R40_OFF_DEF', 'diff_R40_DEF_OFF', 'diff_R40_PACE', 'home_R10_PACE', 'away_R10_PACE', 'home_R20_PACE', 'away_R20_PACE', 'home_R5_PACE', 'away_R5_PACE']
        },
        {
            "Name": "Inj_All_24",
            "Features_List": ['home_R40_OFF_RATING', 'home_R40_DEF_RATING', 'away_R40_OFF_RATING', 'away_R40_DEF_RATING', 'home_elo', 'away_elo', 'elo_diff', 'home_R40_FTA_RATE', 'away_R40_FTA_RATE', 'home_R40_TOV_PCT', 'away_R40_TOV_PCT', 'home_R40_OREB_PCT', 'away_R40_OREB_PCT', 'diff_missing_NET_RATING_r20', 'diff_active_rust_adj_NET_RATING', 'diff_missing_PIE_r20', 'diff_active_rust_adj_PIE', 'home_R10_PACE', 'away_R10_PACE']
        }
    ]
    
    for m in models:
        m['Train_Cols'] = ['vegas_line_h', 'home_team', 'away_team'] + m['Features_List']
        
    print(f"📥 成功載入 {len(models)} 組最強巨頭模型準備進行回測。")
    return models

# ==========================================
# 2. 準備大數據集 (自動調用雲端歷史數據)
# ==========================================
def load_prepared_data():
    print("⏳ [MLOps] 啟動自動數據合體，讀取歷史比賽與數據庫...")
    
    # 透過模組無縫獲取合體後的完整歷史資料
    games_full = get_merged_dataframe("games")
    games = games_full[['game_id', 'date', 'season', 'home_team', 'away_team', 'home_score', 'away_score', 'tw_spread_score']].copy()
    games = games.dropna(subset=['date']).sort_values('date')
    
    elo_dict = {team: 1500.0 for team in set(games['home_team']) | set(games['away_team'])}
    home_elos, away_elos = [], []
    for _, row in games.iterrows():
        h, a = row['home_team'], row['away_team']
        home_elos.append(elo_dict[h])
        away_elos.append(elo_dict[a])
        if pd.notnull(row['home_score']) and pd.notnull(row['away_score']):
            prob_h = 1 / (1 + 10 ** ((elo_dict[a] - (elo_dict[h] + HOME_ADV_ELO)) / 400))
            actual_h = 1.0 if row['home_score'] > row['away_score'] else 0.0
            elo_dict[h] += ELO_K * (actual_h - prob_h)
            elo_dict[a] += ELO_K * ((1 - actual_h) - (1 - prob_h))
    games['home_elo'], games['away_elo'] = home_elos, away_elos
    games['elo_diff'] = games['home_elo'] + HOME_ADV_ELO - games['away_elo']
    
    print("⏳ 讀取並計算球隊滾動特徵...")
    base_stats_full = get_merged_dataframe("boxscore_base")
    base_stats = base_stats_full[['GAME_ID', 'TEAM_ABBREVIATION', 'FGA', 'FTA', 'TOV', 'OREB', 'REB', 'PTS']].rename(columns={'TEAM_ABBREVIATION': 'team'})
    
    adv_stats_full = get_merged_dataframe("boxscore_advanced")
    adv_stats = adv_stats_full[['GAME_ID', 'TEAM_ABBREVIATION', 'OFF_RATING', 'DEF_RATING', 'PACE']].rename(columns={'TEAM_ABBREVIATION': 'team'})
    
    stats = pd.merge(base_stats, adv_stats, on=['GAME_ID', 'team'], how='inner')
    stats['FTA_RATE'] = stats['FTA'] / stats['FGA'].replace(0, 1)
    poss_est = stats['FGA'] + 0.44 * stats['FTA'] + stats['TOV']
    stats['TOV_PCT'] = stats['TOV'] / poss_est.replace(0, 1) * 100
    stats['OREB_PCT'] = stats['OREB'] / stats['REB'].replace(0, 1)
    
    raw_metrics = ['OFF_RATING', 'DEF_RATING', 'PACE', 'FTA_RATE', 'TOV_PCT', 'OREB_PCT']
    stats = stats.merge(games[['game_id', 'date']], left_on='GAME_ID', right_on='game_id', how='left').sort_values(['team', 'date'])
    
    windows = [5, 10, 20, 40]
    for w in windows:
        rolling = stats.groupby('team')[raw_metrics].apply(lambda x: x.shift(1).rolling(window=w, min_periods=5).mean()).reset_index(level=0, drop=True)
        rolling.columns = [f'R{w}_{c}' for c in raw_metrics]
        stats = pd.concat([stats, rolling], axis=1)
    
    all_rolling_cols = [c for c in stats.columns if c.startswith('R')]
    games = games.merge(stats[['GAME_ID', 'team'] + all_rolling_cols], left_on=['game_id', 'home_team'], right_on=['GAME_ID', 'team'], how='left').rename(columns={c: f'home_{c}' for c in all_rolling_cols}).drop(columns=['GAME_ID', 'team'])
    games = games.merge(stats[['GAME_ID', 'team'] + all_rolling_cols], left_on=['game_id', 'away_team'], right_on=['GAME_ID', 'team'], how='left').rename(columns={c: f'away_{c}' for c in all_rolling_cols}).drop(columns=['GAME_ID', 'team'])
    
    for w in windows:
        games[f'diff_R{w}_OFF_DEF'] = games[f'home_R{w}_OFF_RATING'] - games[f'away_R{w}_DEF_RATING']
        games[f'diff_R{w}_DEF_OFF'] = games[f'home_R{w}_DEF_RATING'] - games[f'away_R{w}_OFF_RATING']
        games[f'diff_R{w}_PACE'] = games[f'home_R{w}_PACE'] - games[f'away_R{w}_PACE']

    print("⏳ 合併傷病特徵...")
    if os.path.exists(INJURY_FEATURES_FILE):
        injury_df = pd.read_csv(INJURY_FEATURES_FILE, dtype={'game_id': str})
        injury_cols_to_keep = ['game_id'] + [c for c in injury_df.columns if c.startswith('diff_') and c not in ['home_team', 'away_team']]
        injury_df = injury_df[injury_cols_to_keep]
        
        games['game_id'] = games['game_id'].astype(str).str.zfill(10)
        injury_df['game_id'] = injury_df['game_id'].astype(str).str.zfill(10)
        games = games.merge(injury_df, on='game_id', how='left')
    else:
        print(f"⚠️ 找不到傷病特徵檔: {INJURY_FEATURES_FILE}，請確認是否先執行過 generate_injury.py。")

    games['real_diff'] = games['home_score'] - games['away_score']
    games['vegas_line_h'] = -1 * games['tw_spread_score']
    games['target_residual'] = games['real_diff'] - games['vegas_line_h']
    
    return games.dropna(subset=['tw_spread_score', 'home_elo']).reset_index(drop=True)

# ==========================================
# 3. 主程序：逐日滾動回測 (增量版)
# ==========================================
def run_daily_backtest():
    print("\n" + "="*50)
    print(" 🚀 啟動三巨頭逐日滾動回測 (增量更新版) 🚀 ")
    print("="*50)
    
    models = get_top_models()
    df = load_prepared_data()
    
    # === 🔥 增量更新邏輯 ===
    existing_preds = pd.DataFrame()
    last_processed_date = ""
    
    if os.path.exists(PREDICTIONS_FILE):
        try:
            existing_preds = pd.read_csv(PREDICTIONS_FILE)
            if not existing_preds.empty and 'Date' in existing_preds.columns:
                last_processed_date = str(existing_preds['Date'].max())
                print(f"\n📦 發現既有回測紀錄！最後回測日期為: {last_processed_date}")
        except Exception as e:
            print(f"\n⚠️ 讀取既有紀錄失敗 ({e})，將重新開始回測。")
            existing_preds = pd.DataFrame()

    test_games = df[df['season'] == TEST_SEASON].copy()
    unique_dates = sorted(test_games['date'].unique())
    
    # 篩選出大於最後紀錄日期的「新日期」
    if last_processed_date:
        unique_dates = [d for d in unique_dates if d > last_processed_date]
        
    if len(unique_dates) == 0:
        print("\n✅ 所有日期的比賽都已經回測完畢，預測結果為最新狀態！")
        return
        
    print(f"📅 尚有 {len(unique_dates)} 個新的比賽日需要進行模型訓練與預測。\n")
    # ==========================
    
    all_predictions = []

    # 模擬時光機，只對「新日期」逐日推進
    for current_date in tqdm(unique_dates, desc="📆 新增逐日推進中"):
        historical_data = df[df['date'] < current_date]
        todays_games = df[df['date'] == current_date]
        
        if todays_games.empty:
            continue
            
        # 讓 3 個巨頭模型分別學習並預測
        for m in models:
            feature_cols = m['Train_Cols']
            
            curr_train = historical_data.dropna(subset=feature_cols)
            curr_test = todays_games.dropna(subset=feature_cols)
            
            if curr_test.empty:
                continue
                
            train_split, val_split = train_test_split(curr_train, test_size=0.1, random_state=42)
            
            model = CatBoostRegressor(
                iterations=500, 
                learning_rate=0.03, depth=6, 
                loss_function='RMSE', verbose=False, early_stopping_rounds=30,
                cat_features=['home_team', 'away_team'], allow_writing_files=False
            )
            
            model.fit(
                train_split[feature_cols], train_split['target_residual'], 
                eval_set=(val_split[feature_cols], val_split['target_residual'])
            )
            
            preds = model.predict(curr_test[feature_cols])
            
            for idx, (game_idx, row) in enumerate(curr_test.iterrows()):
                pred_residual = preds[idx]
                real_diff = row['real_diff']
                vegas_line = row['vegas_line_h']
                
                if pred_residual > CONFIDENCE_THRESHOLD:
                    pick = 'Home'
                elif pred_residual < -CONFIDENCE_THRESHOLD:
                    pick = 'Away'
                else:
                    pick = 'Pass'
                    
                home_covered = real_diff > vegas_line
                
                if pick == 'Home':
                    won = 1 if home_covered else 0
                elif pick == 'Away':
                    won = 1 if not home_covered else 0
                else:
                    won = None
                    
                all_predictions.append({
                    'Model_Name': m['Name'],
                    'Date': current_date,
                    'Game_ID': row['game_id'],
                    'Home': row['home_team'],
                    'Away': row['away_team'],
                    'Vegas_Line_H': vegas_line,
                    'Real_Diff': real_diff,
                    'Pred_Residual': round(pred_residual, 2),
                    'Pred_Pick': pick,
                    'Bet_Won': won
                })

    # ==========================================
    # 4. 總結算報告 (合併舊資料與新資料)
    # ==========================================
    if len(all_predictions) > 0:
        print("\n📊 正在合併與結算三巨頭的整體回測成績...")
        
        new_preds_df = pd.DataFrame(all_predictions)
        
        # 結合歷史預測與今日新預測
        if not existing_preds.empty and not new_preds_df.empty:
            final_pred_df = pd.concat([existing_preds, new_preds_df], ignore_index=True)
        elif not existing_preds.empty:
            final_pred_df = existing_preds
        else:
            final_pred_df = new_preds_df
            
        # 將完整紀錄覆寫回 CSV
        final_pred_df.to_csv(PREDICTIONS_FILE, index=False)
        
        results = []
        for m in models:
            m_preds = final_pred_df[final_pred_df['Model_Name'] == m['Name']]
            
            active_bets = m_preds[m_preds['Pred_Pick'] != 'Pass']
            bets_count = len(active_bets)
            
            if bets_count > 0:
                wins = active_bets['Bet_Won'].sum()
                betting_win_pct = wins / bets_count
                est_roi = (betting_win_pct * 0.9) - (1 - betting_win_pct) 
            else:
                betting_win_pct = 0
                est_roi = 0
                
            results.append({
                'Model_Name': m['Name'],
                'Bets_Count': bets_count,
                'Win_Pct': f"{betting_win_pct*100:.2f}%",
                'ROI': f"{est_roi*100:.2f}%"
            })
            
        final_report = pd.DataFrame(results)
        final_report = final_report.sort_values(by='ROI', ascending=False)
        final_report.to_csv(SUMMARY_FILE, index=False)
        
        print("\n" + "="*50)
        print(" 🏆 三巨頭逐日滾動回測 最新總成績 🏆 ")
        print("="*50)
        print(final_report.to_string(index=False))
        print(f"\n✅ 增量回測完畢！完整的每場比賽預測紀錄已更新至 '{PREDICTIONS_FILE}'")
        print(f"   模型總成績已更新至 '{SUMMARY_FILE}'")

if __name__ == "__main__":
    run_daily_backtest()