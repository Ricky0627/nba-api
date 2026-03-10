import pandas as pd
import numpy as np
import os
import json
import sqlite3
import warnings
from datetime import datetime
from xgboost import XGBClassifier

warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ 設定與路徑區
# ==========================================
UPCOMING_CSV = 'data/upcoming_games.csv'
MASTER_FEATURES_CSV = 'data/ml_features_master.csv'
DB_PATH = 'data/nba_current.db' # 👈 新增：資料庫路徑

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
# 🥇 賽後自動結算系統 (直連資料庫 games 表格，結算並回填盤口)
# ==========================================
def settle_past_predictions():
    print("\n" + "="*60)
    print("🔄 啟動賽後自動結算系統 (啟動資料庫 games 表格深度挖掘)...")
    
    if not os.path.exists(OUTPUT_PREDICTION):
        print("   ℹ️ 尚未產生任何大小分預測紀錄檔，跳過結算。")
        return

    # 讀取預測紀錄
    preds_df = pd.read_csv(OUTPUT_PREDICTION)
    
    # 若沒有 Is_Win 或 Line 欄位，自動加上
    if 'Is_Win' not in preds_df.columns:
        preds_df['Is_Win'] = np.nan
    if 'Line' not in preds_df.columns:
        preds_df['Line'] = '未開盤'

    # 找出尚未結算的，或是盤口是空的注單
    unsettled_mask = preds_df['Is_Win'].isna()
    missing_line_mask = preds_df['Line'].isna() | (preds_df['Line'] == '未開盤') | (preds_df['Line'] == '') | (preds_df['Line'] == 'nan')
    
    if not unsettled_mask.any() and not missing_line_mask.any():
        print("   ✅ 所有歷史預測皆已結算且盤口完整！")
        return

    # 🚀 直接連線資料庫讀取 games 表格 (最準確的原始來源)
    actual_results = {}
    if os.path.exists(DB_PATH):
        try:
            conn = sqlite3.connect(DB_PATH)
            games_df = pd.read_sql("SELECT * FROM games", conn)
            conn.close()
            
            # 🔥 防呆 1：強制將資料庫欄位轉小寫，避免 KeyError
            games_df.columns = [c.lower() for c in games_df.columns]
            
            # 統一日期格式
            games_df['DATE_STR'] = pd.to_datetime(games_df['game_date']).dt.strftime('%Y-%m-%d')
            
            for _, row in games_df.iterrows():
                matchup = f"{row['away_team']} @ {row['home_team']}"
                key = f"{matchup}_{row['DATE_STR']}"
                
                ou_winner = None
                try:
                    # 🔥 防呆 2：安全轉換數字，避免空字串轉 float 崩潰
                    home_score = float(row.get('home_score', 0))
                    away_score = float(row.get('away_score', 0))
                    # 👇 依指示將資料庫欄位精準改為 tw_total_score
                    tw_total = float(row.get('tw_total_score', 0))
                    
                    if home_score > 0 and away_score > 0 and tw_total > 0:
                        total_pts = home_score + away_score
                        # 🔥 防呆 3：正確處理走水 (剛好等於盤口)
                        if total_pts > tw_total:
                            ou_winner = 'OVER'
                        elif total_pts < tw_total:
                            ou_winner = 'UNDER'
                        else:
                            ou_winner = None # 走水不計勝負
                except Exception:
                    pass

                actual_results[key] = {
                    'ou_winner': ou_winner,
                    # 👇 回填時也確保抓取正確的 tw_total_score
                    'ou_line': row.get('tw_total_score')
                }
        except Exception as e:
            print(f"   ⚠️ 讀取資料庫 games 表格時發生錯誤: {e}")
    else:
        print(f"   ⚠️ 找不到資料庫 {DB_PATH}，無法進行深度挖掘。")
        return
    
    settled_count = 0
    fixed_line_count = 0
    
    # 開始配對並結算
    for idx, row in preds_df.iterrows():
        # 如果勝負已結算 且 盤口也抓到了，就跳過
        if pd.notna(row.get('Is_Win')) and str(row.get('Line', '')).strip() not in ['', 'nan', 'NaN', '未開盤', 'None']:
            continue

        try:
            pred_date_str = pd.to_datetime(str(row['Game_Date']).strip()).strftime('%Y-%m-%d')
        except:
            pred_date_str = str(row['Game_Date']).strip()

        key = f"{row['Matchup']}_{pred_date_str}"
        matched_data = actual_results.get(key)

        # 時區容錯：時區問題找前後一天
        if not matched_data:
            try:
                pred_dt = pd.to_datetime(pred_date_str)
                key_prev = f"{row['Matchup']}_{(pred_dt - pd.Timedelta(days=1)).strftime('%Y-%m-%d')}"
                key_next = f"{row['Matchup']}_{(pred_dt + pd.Timedelta(days=1)).strftime('%Y-%m-%d')}"
                
                matched_data = actual_results.get(key_prev) or actual_results.get(key_next)
            except:
                pass

        # 如果成功配對到賽果，進行結算與回填
        if matched_data:
            current_line = str(row.get('Line', '')).strip()
            
            # 結算勝負
            if pd.isna(row.get('Is_Win')) and matched_data['ou_winner']:
                preds_df.at[idx, 'Is_Win'] = 1 if row['Predicted_OU'] == matched_data['ou_winner'] else 0
                settled_count += 1
                
            # 🔥 自動回填盤口機制：如果當初沒抓到盤口，結算時順便從資料庫補回去
            if current_line in ['', 'nan', 'NaN', '未開盤', 'None'] and pd.notna(matched_data['ou_line']):
                preds_df.at[idx, 'Line'] = matched_data['ou_line']
                fixed_line_count += 1

    if settled_count > 0 or fixed_line_count > 0:
        preds_df.to_csv(OUTPUT_PREDICTION, index=False, encoding='utf-8-sig')
        print(f"   💰 挖掘完成！結算了 {settled_count} 筆賽果，並成功從資料庫搶救回填了 {fixed_line_count} 筆歷史盤口數字。")
    else:
        print("   ⏳ 尚無最新賽果可供結算 (比賽可能還沒打完，或遇到走水/未開盤)。")
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
        # 🔥 強制將欄位名稱去空白並轉大寫 (防禦隱形空白殺手)
        row_upper = {str(k).strip().upper(): v for k, v in row.items()}
        
        home_team = row_upper.get('HOME_TEAM')
        away_team = row_upper.get('AWAY_TEAM')
        game_date = row_upper.get('GAME_DATE')
        matchup_name = f"{away_team} @ {home_team}"
        
        home_features = home_stats_dict.get(home_team, {})
        away_features = away_stats_dict.get(away_team, {})
        
        # 🎯 擴大搜尋範圍，精準抓取維加斯大小分盤口 VEGAS_TOTAL
        possible_ou_cols = ['VEGAS_TOTAL', 'TW_TOTAL_SCORE', 'TW_TOTAL', 'TW_OU_SCORE', 'TOTAL']
        game_line = '未開盤'
        for col in possible_ou_cols:
            if col in row_upper:
                val = row_upper[col]
                if pd.notna(val) and str(val).strip() not in ['', 'nan', 'NaN', 'None']:
                    game_line = str(val).strip()
                    break

        today_context = {
            "HOME_IS_B2B": 1 if str(row_upper.get('HOME_IS_B2B', 'False')).strip().lower() == 'true' else 0,
            "AWAY_IS_B2B": 1 if str(row_upper.get('AWAY_IS_B2B', 'False')).strip().lower() == 'true' else 0,
            "HOME_REST_DAYS": row_upper.get('HOME_REST_DAYS', 2), 
            "AWAY_REST_DAYS": row_upper.get('AWAY_REST_DAYS', 2)
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
            
            model_obj = models[m_name]
            prob = model_obj.predict_proba(X_df)[0]
            
            # 🔥 防呆 4：動態尋找 OVER 或 1 的索引，避免 XGBoost 字母排序反轉
            classes = list(model_obj.classes_)
            if 'OVER' in classes:
                over_idx = classes.index('OVER')
            elif 1 in classes:
                over_idx = classes.index(1)
            else:
                over_idx = 1 # Fallback
            
            prob_over = prob[over_idx]
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
    # 1. 先執行結算，把過去的單子對獎 (順便去資料庫回填漏抓的 Line 盤口)
    settle_past_predictions()
    
    # 2. 再執行今日賽事預測 (嚴格從 upcoming_games.csv 抓盤口)
    predict_upcoming_games()