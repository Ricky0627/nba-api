import pandas as pd
import numpy as np
import os
import json
import warnings
from datetime import datetime
from xgboost import XGBClassifier

warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ 設定與路徑區
# ==========================================
UPCOMING_CSV = 'data/upcoming_games.csv'
MASTER_FEATURES_CSV = 'data/ml_features_master.csv'

# 這裡要對齊我們剛才設定的資料夾！
JSON_MODELS_DIR = 'models_ou/'              # 👈 這是讀取特徵清單的路徑
MODEL_DIR = 'trained_models_ou/'            # 👈 這是讀取 XGBoost 權重的路徑

OUTPUT_PREDICTION = 'data/ou_predictions_history_log.csv'
# ==========================================
# 🏆 動態載入大小分神級特徵 (從 JSON)
# ==========================================
def load_ou_models_config():
    ou_models = []
    if not os.path.exists(JSON_MODELS_DIR):
        print(f"⚠️ 找不到 JSON 設定資料夾 {JSON_MODELS_DIR}，請確認路徑。")
        return ou_models
        
    for filename in os.listdir(JSON_MODELS_DIR):
        if filename.endswith('.json'):
            filepath = os.path.join(JSON_MODELS_DIR, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                model_config = json.load(f)
                ou_models.append(model_config)
    return ou_models

ALL_MODELS = load_ou_models_config()

# ==========================================
# 🥇 賽後自動結算系統 (大小分專用 - 加入盤口回填)
# ==========================================
def settle_past_predictions():
    print("\n" + "="*60)
    print("🔄 啟動賽後自動結算系統 (核對大小分結果與回填盤口)...")
    
    if not os.path.exists(OUTPUT_PREDICTION):
        print("   ℹ️ 尚未產生任何大小分預測紀錄檔，跳過結算。")
        return

    # 讀取預測紀錄
    preds_df = pd.read_csv(OUTPUT_PREDICTION)
    
    # 若沒有 Is_Win 欄位，自動加上
    if 'Is_Win' not in preds_df.columns:
        preds_df['Is_Win'] = np.nan

    # 找出尚未結算的注單
    unsettled_mask = preds_df['Is_Win'].isna()
    if not unsettled_mask.any():
        print("   ✅ 所有歷史預測皆已結算完畢！")
        return

    try:
        # 讀取最新特徵大表 (裡面包含已打完的賽事分數與大小分盤口)
        df_master = pd.read_csv(MASTER_FEATURES_CSV, low_memory=False)
        df_master.columns = [c.upper() for c in df_master.columns]

        if 'HOME_SCORE' not in df_master.columns or 'TW_TOTAL_SCORE' not in df_master.columns:
            print("   ⚠️ 特徵大表中缺乏分數或 TW_TOTAL_SCORE 欄位，無法自動結算。")
            return

        # 濾出已經打完、且有盤口的賽事
        completed_games = df_master.dropna(subset=['HOME_SCORE', 'AWAY_SCORE', 'TW_TOTAL_SCORE']).copy()
        completed_games['TW_TOTAL_SCORE'] = pd.to_numeric(completed_games['TW_TOTAL_SCORE'], errors='coerce')
        
        # 🔥 計算大小分賽果
        completed_games['TOTAL_PTS'] = completed_games['HOME_SCORE'] + completed_games['AWAY_SCORE']
        
        # 剔除走水(剛好等於盤口)的比賽，不列入計算
        completed_games = completed_games[completed_games['TOTAL_PTS'] != completed_games['TW_TOTAL_SCORE']]
        
        # 判斷真實結果是 OVER 還是 UNDER
        completed_games['ACTUAL_OU'] = np.where(completed_games['TOTAL_PTS'] > completed_games['TW_TOTAL_SCORE'], 'OVER', 'UNDER')

        # 找出日期欄位
        date_col = 'GAME_DATE' if 'GAME_DATE' in completed_games.columns else ('DATE' if 'DATE' in completed_games.columns else None)
        if not date_col:
            print("   ⚠️ 找不到日期欄位，無法配對比賽。")
            return

        # 統一日期格式 YYYY-MM-DD
        completed_games['DATE_STR'] = pd.to_datetime(completed_games[date_col]).dt.strftime('%Y-%m-%d')

        # 建立「賽果字典」：對戰組合_日期 -> {winner: 實際大小分結果, line: 最終盤口}
        actual_results = {}
        for _, row in completed_games.iterrows():
            matchup = f"{row['AWAY_TEAM']} @ {row['HOME_TEAM']}"
            key = f"{matchup}_{row['DATE_STR']}"
            actual_results[key] = {
                'winner': row['ACTUAL_OU'],
                'line': row['TW_TOTAL_SCORE']
            }

        # 開始配對並結算
        settled_count = 0
        for idx, row in preds_df[unsettled_mask].iterrows():
            try:
                pred_date_str = pd.to_datetime(str(row['Game_Date']).strip()).strftime('%Y-%m-%d')
            except:
                pred_date_str = str(row['Game_Date']).strip()

            key = f"{row['Matchup']}_{pred_date_str}"
            matched_data = None

            if key in actual_results:
                matched_data = actual_results[key]
            else:
                # 容錯機制：時區問題找前後一天
                try:
                    pred_dt = pd.to_datetime(pred_date_str)
                    key_prev = f"{row['Matchup']}_{(pred_dt - pd.Timedelta(days=1)).strftime('%Y-%m-%d')}"
                    key_next = f"{row['Matchup']}_{(pred_dt + pd.Timedelta(days=1)).strftime('%Y-%m-%d')}"
                    
                    if key_prev in actual_results: 
                        matched_data = actual_results[key_prev]
                    elif key_next in actual_results: 
                        matched_data = actual_results[key_next]
                except:
                    pass

            # 如果成功配對到賽果，進行結算
            if matched_data:
                # 如果模型預測 (OVER/UNDER) == 實際賽果 (OVER/UNDER)，Is_Win 填入 1，否則填 0
                preds_df.at[idx, 'Is_Win'] = 1 if row['Predicted_OU'] == matched_data['winner'] else 0
                
                # 🔥 自動回填盤口機制：如果當初沒抓到盤口（空值或未開盤），結算時順便從大表補回去！
                current_line = str(row.get('Line', '')).strip()
                if current_line in ['', 'nan', 'NaN', '未開盤', 'None']:
                    preds_df.at[idx, 'Line'] = matched_data['line']
                    
                settled_count += 1

        if settled_count > 0:
            preds_df.to_csv(OUTPUT_PREDICTION, index=False, encoding='utf-8-sig')
            print(f"   💰 結算完成！成功為 {settled_count} 筆大小分歷史注單更新了賽果與盤口。")
        else:
            print("   ⏳ 尚無最新賽果可供結算 (比賽可能還沒打完，或遇到走水/未開盤)。")

    except Exception as e:
        print(f"   ❌ 結算過程中發生錯誤: {e}")
    print("="*60)

# ==========================================
# 🔍 載入特徵與預測核心
# ==========================================
def load_latest_features():
    print("🔍 正在從特徵大表提取各隊最新實力指標...")
    df_master = pd.read_csv(MASTER_FEATURES_CSV, low_memory=False)
    
    # 1. 統一轉大寫
    df_master.columns = [c.upper() for c in df_master.columns]

    # 2. 自動變形器：把大表的 _HOME, _AWAY 後綴轉換成 HOME_, AWAY_ 前綴
    new_cols = []
    for c in df_master.columns:
        if c.endswith('_HOME'):
            new_cols.append('HOME_' + c[:-5])
        elif c.endswith('_AWAY'):
            new_cols.append('AWAY_' + c[:-5])
        else:
            new_cols.append(c)
    df_master.columns = new_cols
    
    df_master = df_master.fillna(0)
    
    # 取出各球隊「最新一場」的特徵數據
    latest_home = df_master.drop_duplicates(subset=['HOME_TEAM'], keep='last').copy()
    latest_away = df_master.drop_duplicates(subset=['AWAY_TEAM'], keep='last').copy()
    
    team_latest_home_stats = latest_home.set_index('HOME_TEAM').to_dict('index')
    team_latest_away_stats = latest_away.set_index('AWAY_TEAM').to_dict('index')
    
    return team_latest_home_stats, team_latest_away_stats

def predict_upcoming_games():
    if not ALL_MODELS:
        print("❌ 找不到任何大小分模型設定，預測中止。")
        return
        
    print(f"🚀 啟動 NBA 【大小分】{len(ALL_MODELS)}神聯軍全預測系統！")
    
    if not os.path.exists(UPCOMING_CSV):
        print("❌ 找不到今日賽程 (upcoming_games.csv)！今日可能無賽事。")
        return
        
    upcoming_df = pd.read_csv(UPCOMING_CSV)
    if upcoming_df.empty:
        print("🤷‍♂️ 今日無賽事需要預測。")
        return
        
    home_stats_dict, away_stats_dict = load_latest_features()
    
    # 讀取 .json 對應的 XGBoost 模型權重檔 (.json)
    models = {}
    for stage in ALL_MODELS:
        m_name = stage['name']
        # 注意：這裡假設你將 XGBoost 的 weights 存成了 m_name + '.json'
        model_path = os.path.join(MODEL_DIR, f"{m_name}.json") 
        if os.path.exists(model_path):
            model = XGBClassifier()
            model.load_model(model_path)
            models[m_name] = model
        else:
            print(f"⚠️ 警告: 找不到 XGBoost 模型權重檔 {model_path}，將跳過此模型的預測。")
            
    if not models:
        print("❌ 沒有讀取到任何 XGBoost 實體模型，預測中止。")
        return

    predictions_log = []
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"\n🎯 今日共有 {len(upcoming_df)} 場賽事，開始進行大小分全面分析...\n" + "="*60)
    
    for _, row in upcoming_df.iterrows():
        home_team = row['home_team']
        away_team = row['away_team']
        game_date = row['game_date']
        matchup_name = f"{away_team} @ {home_team}"
        
        home_features = home_stats_dict.get(home_team, {})
        away_features = away_stats_dict.get(away_team, {})
        
        # 🎯 新增：抓取今日大小分盤口數字
        game_line = row.get('tw_total', row.get('TW_TOTAL_SCORE'))
        if pd.isna(game_line) or game_line == '':
            game_line = home_features.get('TW_TOTAL_SCORE', '未開盤')
        if pd.isna(game_line) or game_line == '':
            game_line = '未開盤'

        today_context = {
            "HOME_IS_B2B": 1 if row.get('home_is_b2b', False) else 0,
            "AWAY_IS_B2B": 1 if row.get('away_is_b2b', False) else 0,
            "HOME_REST_DAYS": row.get('home_rest_days', 2), 
            "AWAY_REST_DAYS": row.get('away_rest_days', 2)
        }
        
        X_input = {}
        
        # 🔥 嚴格過濾：主隊只拿 HOME 特徵，客隊只拿 AWAY 特徵
        for k, v in home_features.items():
            if k.startswith('HOME_'):
                X_input[k] = v
                
        for k, v in away_features.items():
            if k.startswith('AWAY_'):
                X_input[k] = v
                
        X_input.update(today_context)
        
        print(f"🏀 {matchup_name} (大小盤口: {game_line})")
        
        for stage in ALL_MODELS:
            m_name = stage['name']
            if m_name not in models: continue
            
            features_list = stage['features']
            X_model_dict = {f: X_input.get(f, 0) for f in features_list}
            X_df = pd.DataFrame([X_model_dict])[features_list].astype('float32')
            
            prob = models[m_name].predict_proba(X_df)[0]
            
            # 🔥 大小分邏輯：prob[1] 是 OVER(大分) 的機率，prob[0] 是 UNDER(小分) 的機率
            prob_over = prob[1]
            confidence = max(prob[0], prob[1])
            predicted_ou = "OVER" if prob_over >= 0.5 else "UNDER"
            
            prediction_record = {
                "Run_Time": run_timestamp,
                "Game_Date": game_date,
                "Matchup": matchup_name,
                "Model_Used": m_name,
                "Track_Name": stage['track'],
                "Predicted_OU": predicted_ou,
                "Confidence_Pct": round(confidence * 100, 2),
                "Is_Win": np.nan, # 新預測的單子，預設為未結算
                "Line": game_line # 👈 新增：把大小分盤數字寫入 CSV
            }
            predictions_log.append(prediction_record)
            
            print(f"   📊 [{m_name:<15} | {stage['track']:<15}] 預測: {predicted_ou:<5} (信心: {round(confidence*100, 2)}%)")
            
        print("-" * 60)

    # ==========================================
    # 💾 智慧覆蓋與追加 (Upsert) 寫入 CSV
    # ==========================================
    if predictions_log:
        df_new = pd.DataFrame(predictions_log)
        os.makedirs(os.path.dirname(OUTPUT_PREDICTION), exist_ok=True)
        
        if os.path.exists(OUTPUT_PREDICTION):
            try:
                df_history = pd.read_csv(OUTPUT_PREDICTION)
                df_combined = pd.concat([df_history, df_new], ignore_index=True)
                # 以 日期+對戰+模型名稱 作為唯一鍵值，若重複則用最新預測覆蓋
                df_combined = df_combined.drop_duplicates(subset=['Game_Date', 'Matchup', 'Model_Used'], keep='last')
            except Exception as e:
                print(f"⚠️ 讀取歷史紀錄失敗: {e}")
                df_combined = df_new
        else:
            df_combined = df_new
            
        df_combined.to_csv(OUTPUT_PREDICTION, index=False, encoding='utf-8-sig')
        print(f"\n✅ 今日大小分預測完畢！總計 {len(predictions_log)} 筆結果已成功【更新/追加】至: {OUTPUT_PREDICTION}")

if __name__ == "__main__":
    # 1. 先執行結算，把過去的單子對獎 (順便回填漏抓的 Line 盤口)
    settled = settle_past_predictions()
    
    # 2. 再執行今日賽事預測
    predict_upcoming_games()