import pandas as pd
import numpy as np
import time
import re
import sqlite3
import os
import warnings

# 忽略 Pandas 的警告，保持 GitHub Actions 的 Log 乾淨
warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ 雲端自動化設定區 (PBP 五大進階特徵工廠)
# ==========================================
# 改為指向輕量級的當前賽季資料
DB_PATH = 'data/nba_current.db'
INPUT_CSV = 'data/current_play_by_play.csv'

# 定義五大特徵對應的資料表名稱
TABLES = {
    'clutch': 'team_features_clutch',
    'shot': 'team_features_shot_profile',
    'tov': 'team_features_turnover',
    'momentum': 'team_features_momentum',
    'quarterly': 'team_features_quarterly'
}

def extract_time_in_seconds(clock_str):
    if pd.isna(clock_str): return 0
    clock_str = str(clock_str).strip()
    if clock_str.startswith('PT'):
        m_match = re.search(r'PT(\d+)M', clock_str)
        s_match = re.search(r'M(\d+\.?\d*)S', clock_str)
        if not m_match and s_match: return float(s_match.group(1))
        mins = int(m_match.group(1)) if m_match else 0
        secs = float(s_match.group(1)) if s_match else 0
        return mins * 60 + secs
    elif ':' in clock_str:
        parts = clock_str.split(':')
        if len(parts) == 2: return int(parts[0]) * 60 + float(parts[1])
    return 0

def get_existing_gids(conn, table_name):
    """從資料庫獲取已經處理過的 GAME_ID"""
    try:
        query = f"SELECT DISTINCT GAME_ID FROM {table_name}"
        df_existing = pd.read_sql(query, conn)
        return set(df_existing['GAME_ID'].astype(str).str.zfill(10).tolist())
    except Exception:
        # 表格不存在代表是全新的
        return set()

def build_all_pbp_features_to_db():
    print(f"📥 啟動 PBP 特徵自動化提煉工廠 (雲端增量版)...")
    start_time = time.time()

    # 1️⃣ 讀取當前賽季的 PBP CSV
    if not os.path.exists(INPUT_CSV):
        print(f"❌ 找不到 PBP 檔案：{INPUT_CSV}，請確認爬蟲是否已執行。")
        return

    print(f"⏳ 正在讀取 PBP 資料庫...")
    df = pd.read_csv(INPUT_CSV, low_memory=False)
    
    if df.empty:
        print("⚠️ PBP 檔案為空，無新資料需處理。")
        return
        
    df['GAME_ID'] = df['GAME_ID'].astype(str).str.zfill(10)
    df['teamId'] = pd.to_numeric(df['teamId'], errors='coerce').fillna(0).astype(int)
    
    # 確保有順序且只留有效球隊
    df = df[df['teamId'] > 0].sort_values(by=['GAME_ID', 'actionNumber']).reset_index(drop=True)
    
    # ==========================================
    # 🛠️ 全域預處理 (只算一次，加速後續 5 個模組)
    # ==========================================
    print("   🧹 正在進行全域動作解析標籤化...")
    action_str = df['actionType'].astype(str).str.upper().str.strip()
    desc_str = df['description'].astype(str).str.upper()

    df['FGM'] = (action_str == 'MADE SHOT').astype(int)
    df['FGA'] = (action_str.isin(['MADE SHOT', 'MISSED SHOT'])).astype(int)
    df['is_3PT'] = ((df['FGA'] == 1) & (desc_str.str.contains('3PT|3-PT|3 PT'))).astype(int)
    df['FG3M'] = ((df['FGM'] == 1) & (df['is_3PT'] == 1)).astype(int)
    df['FTA'] = (action_str == 'FREE THROW').astype(int)
    df['FTM'] = ((df['FTA'] == 1) & (~desc_str.str.contains('MISS'))).astype(int)
    df['TOV'] = (action_str == 'TURNOVER').astype(int)
    df['FOUL'] = (action_str == 'FOUL').astype(int)
    df['REB'] = (action_str == 'REBOUND').astype(int)
    df['PTS'] = (df['FGM'] * 2) + df['FG3M'] + df['FTM']
    df['is_POSS_ending'] = (df['FGA'] == 1) | (df['FTA'] == 1) | (df['TOV'] == 1)

    # 開啟資料庫連線，加入 timeout 防止寫入鎖定
    conn = sqlite3.connect(DB_PATH, timeout=20.0)

    try:
        # ==========================================
        # 1️⃣ 關鍵時刻特徵 (Clutch Time)
        # ==========================================
        table = TABLES['clutch']
        completed = get_existing_gids(conn, table)
        new_gids = set(df['GAME_ID'].unique()) - completed
        
        if new_gids:
            print(f"\n🔥 [1/5] {table}: 發現 {len(new_gids)} 場新比賽，開始處理...")
            df_new = df[df['GAME_ID'].isin(new_gids)].copy()
            base_df = df_new[['GAME_ID', 'teamId']].drop_duplicates()
            
            df_new['scoreHome'] = pd.to_numeric(df_new['scoreHome'], errors='coerce')
            df_new['scoreAway'] = pd.to_numeric(df_new['scoreAway'], errors='coerce')
            df_new['scoreHome'] = df_new.groupby('GAME_ID')['scoreHome'].ffill().fillna(0)
            df_new['scoreAway'] = df_new.groupby('GAME_ID')['scoreAway'].ffill().fillna(0)
            df_new['score_diff'] = abs(df_new['scoreHome'] - df_new['scoreAway'])
            df_new['remaining_seconds'] = df_new['clock'].apply(extract_time_in_seconds)

            is_clutch = (df_new['period'] >= 4) & (df_new['remaining_seconds'] <= 300) & (df_new['score_diff'] <= 5)
            clutch_logs = df_new[is_clutch].groupby(['GAME_ID', 'teamId']).agg(
                Clutch_FGM=('FGM', 'sum'), Clutch_FGA=('FGA', 'sum'),
                Clutch_FTA=('FTA', 'sum'), Clutch_FTM=('FTM', 'sum'),
                Clutch_TOV=('TOV', 'sum'), Clutch_PTS=('PTS', 'sum')
            ).reset_index()

            if not clutch_logs.empty:
                ts_den = 2 * (clutch_logs['Clutch_FGA'] + 0.44 * clutch_logs['Clutch_FTA']) + 1e-6
                clutch_logs['Clutch_TS_pct'] = (clutch_logs['Clutch_PTS'] / ts_den).round(4)
                tov_den = clutch_logs['Clutch_FGA'] + 0.44 * clutch_logs['Clutch_FTA'] + clutch_logs['Clutch_TOV'] + 1e-6
                clutch_logs['Clutch_TOV_pct'] = (clutch_logs['Clutch_TOV'] / tov_den).round(4)
            
            final_clutch = base_df.merge(clutch_logs, on=['GAME_ID', 'teamId'], how='left').fillna(0)
            final_clutch.rename(columns={'teamId': 'TEAM_ID'}).to_sql(table, conn, if_exists='append', index=False)
            print(f"   ✔️ 成功寫入 {len(final_clutch)} 筆。")
        else:
            print(f"✅ [1/5] {table} 已是最新。")

        # ==========================================
        # 2️⃣ 投籃空間與魔球指數 (Shot Profile)
        # ==========================================
        table = TABLES['shot']
        completed = get_existing_gids(conn, table)
        new_gids = set(df['GAME_ID'].unique()) - completed
        
        if new_gids:
            print(f"\n🎯 [2/5] {table}: 發現 {len(new_gids)} 場新比賽，開始處理...")
            df_new = df[df['GAME_ID'].isin(new_gids)].copy()
            base_df = df_new[['GAME_ID', 'teamId']].drop_duplicates()
            
            fga_df = df_new[df_new['FGA'] == 1].copy()
            fga_df['shotDistance'] = pd.to_numeric(fga_df['shotDistance'], errors='coerce').fillna(0).astype(float)
            fga_df['is_Rim'] = ((fga_df['shotDistance'] <= 4) & (fga_df['is_3PT'] == 0)).astype(int)
            fga_df['is_Mid'] = ((fga_df['shotDistance'] > 4) & (fga_df['is_3PT'] == 0)).astype(int)

            shot_logs = fga_df.groupby(['GAME_ID', 'teamId']).agg(
                Total_FGA=('FGA', 'sum'), Rim_FGA=('is_Rim', 'sum'),
                Mid_FGA=('is_Mid', 'sum'), FG3A=('is_3PT', 'sum')
            ).reset_index()

            if not shot_logs.empty:
                total_fga = shot_logs['Total_FGA'] + 1e-6
                shot_logs['Rim_FREQ'] = (shot_logs['Rim_FGA'] / total_fga).round(4)
                shot_logs['Mid_FREQ'] = (shot_logs['Mid_FGA'] / total_fga).round(4)
                shot_logs['FG3A_FREQ'] = (shot_logs['FG3A'] / total_fga).round(4)
                shot_logs['Moreyball_Index'] = (shot_logs['Rim_FREQ'] + shot_logs['FG3A_FREQ']).round(4)

            final_shot = base_df.merge(shot_logs, on=['GAME_ID', 'teamId'], how='left').fillna(0)
            final_shot.rename(columns={'teamId': 'TEAM_ID'}).to_sql(table, conn, if_exists='append', index=False)
            print(f"   ✔️ 成功寫入 {len(final_shot)} 筆。")
        else:
            print(f"✅ [2/5] {table} 已是最新。")

        # ==========================================
        # 3️⃣ 失誤體質特徵 (Turnover Profile)
        # ==========================================
        table = TABLES['tov']
        completed = get_existing_gids(conn, table)
        new_gids = set(df['GAME_ID'].unique()) - completed
        
        if new_gids:
            print(f"\n🤦‍♂️ [3/5] {table}: 發現 {len(new_gids)} 場新比賽，開始處理...")
            df_new = df[df['GAME_ID'].isin(new_gids)].copy()
            base_df = df_new[['GAME_ID', 'teamId']].drop_duplicates()
            
            tov_df = df_new[df_new['TOV'] == 1].copy()
            tov_df['Live_TOV'] = tov_df['subType'].astype(str).str.upper().str.strip().isin(['BAD PASS', 'LOST BALL']).astype(int)
            tov_df['Dead_TOV'] = (tov_df['Live_TOV'] == 0).astype(int)
            tov_df['Total_TOV'] = 1

            tov_logs = tov_df.groupby(['GAME_ID', 'teamId']).agg(
                Total_TOV=('Total_TOV', 'sum'), Live_TOV=('Live_TOV', 'sum'), Dead_TOV=('Dead_TOV', 'sum')
            ).reset_index()

            if not tov_logs.empty:
                total_tov = tov_logs['Total_TOV'] + 1e-6
                tov_logs['Live_TOV_pct'] = (tov_logs['Live_TOV'] / total_tov).round(4)
                tov_logs['Dead_TOV_pct'] = (tov_logs['Dead_TOV'] / total_tov).round(4)

            final_tov = base_df.merge(tov_logs, on=['GAME_ID', 'teamId'], how='left').fillna(0)
            final_tov.rename(columns={'teamId': 'TEAM_ID'}).to_sql(table, conn, if_exists='append', index=False)
            print(f"   ✔️ 成功寫入 {len(final_tov)} 筆。")
        else:
            print(f"✅ [3/5] {table} 已是最新。")

        # ==========================================
        # 4️⃣ 一波流與動能特徵 (Momentum Tracker)
        # ==========================================
        table = TABLES['momentum']
        completed = get_existing_gids(conn, table)
        new_gids = set(df['GAME_ID'].unique()) - completed
        
        if new_gids:
            print(f"\n🌊 [4/5] {table}: 發現 {len(new_gids)} 場新比賽，開始處理...")
            df_new = df[df['GAME_ID'].isin(new_gids)].copy()
            base_df = df_new[['GAME_ID', 'teamId']].drop_duplicates()
            
            scores = df_new[df_new['PTS'] > 0].copy()
            scores['team_change'] = ((scores['teamId'] != scores['teamId'].shift(1)) | 
                                     (scores['GAME_ID'] != scores['GAME_ID'].shift(1))).cumsum()
            
            runs = scores.groupby(['GAME_ID', 'teamId', 'team_change']).agg(
                Run_PTS=('PTS', 'sum'), End_Action=('actionNumber', 'max')
            ).reset_index()

            momentum_logs = runs.groupby(['GAME_ID', 'teamId']).agg(
                Max_Unanswered_Run=('Run_PTS', 'max'),
                Runs_10_0_Count=('Run_PTS', lambda x: (x >= 10).sum())
            ).reset_index()

            game_teams_dict = df_new.groupby('GAME_ID')['teamId'].unique().to_dict()
            big_runs = runs[runs['Run_PTS'] >= 8]
            recovery_records = []
            
            if not big_runs.empty:
                poss_df = df_new[df_new['is_POSS_ending']].copy()
                poss_groups = poss_df.groupby('GAME_ID')
                for _, run_row in big_runs.iterrows():
                    g_id, run_team, end_act = run_row['GAME_ID'], run_row['teamId'], run_row['End_Action']
                    opp_teams = [t for t in game_teams_dict.get(g_id, []) if t != run_team]
                    if not opp_teams: continue
                    opp_team = opp_teams[0] 
                    
                    if g_id in poss_groups.groups:
                        g_df = poss_groups.get_group(g_id)
                        post_run_df = g_df[(g_df['teamId'] == opp_team) & (g_df['actionNumber'] > end_act)]
                        next_3_poss = post_run_df.head(3)
                        if len(next_3_poss) > 0:
                            recovery_records.append({
                                'GAME_ID': g_id, 'teamId': opp_team,
                                'Recovery_PTS': next_3_poss['PTS'].sum(), 'Possessions': len(next_3_poss)
                            })

            if recovery_records:
                rec_agg = pd.DataFrame(recovery_records).groupby(['GAME_ID', 'teamId']).agg(
                    Total_Recovery_PTS=('Recovery_PTS', 'sum'), Total_Recovery_Poss=('Possessions', 'sum')
                ).reset_index()
                rec_agg['Run_Deficit_Recovery_Rate'] = (rec_agg['Total_Recovery_PTS'] / rec_agg['Total_Recovery_Poss']).round(4)
                momentum_logs = pd.merge(momentum_logs, rec_agg[['GAME_ID', 'teamId', 'Run_Deficit_Recovery_Rate']], on=['GAME_ID', 'teamId'], how='left')
            else:
                momentum_logs['Run_Deficit_Recovery_Rate'] = np.nan

            final_momentum = base_df.merge(momentum_logs, on=['GAME_ID', 'teamId'], how='left').fillna(0)
            final_momentum.rename(columns={'teamId': 'TEAM_ID'}).to_sql(table, conn, if_exists='append', index=False)
            print(f"   ✔️ 成功寫入 {len(final_momentum)} 筆。")
        else:
            print(f"✅ [4/5] {table} 已是最新。")

        # ==========================================
        # 5️⃣ 每節進階數據 (Quarterly Features)
        # ==========================================
        table = TABLES['quarterly']
        completed = get_existing_gids(conn, table)
        new_gids = set(df['GAME_ID'].unique()) - completed
        
        if new_gids:
            print(f"\n⏱️ [5/5] {table}: 發現 {len(new_gids)} 場新比賽，開始處理...")
            df_new = df[df['GAME_ID'].isin(new_gids)].copy()
            
            df_new['period_label'] = df_new['period'].apply(lambda x: f"Q{x}" if x <= 4 else "OT")
            quarterly_stats = df_new.groupby(['GAME_ID', 'teamId', 'period_label']).agg(
                PTS=('PTS', 'sum'), FGM=('FGM', 'sum'), FGA=('FGA', 'sum'),
                FG3M=('FG3M', 'sum'), FG3A=('is_3PT', 'sum'), FTM=('FTM', 'sum'),
                FTA=('FTA', 'sum'), TOV=('TOV', 'sum'), FOUL=('FOUL', 'sum'), REB=('REB', 'sum')
            ).reset_index()

            wide_df = quarterly_stats.pivot_table(
                index=['GAME_ID', 'teamId'], columns='period_label', 
                values=['PTS', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'TOV', 'FOUL', 'REB'], fill_value=0
            )
            wide_df.columns = [f"{period}_{stat}" for stat, period in wide_df.columns]
            quarterly_logs = wide_df.reset_index()

            expected_periods = ['Q1', 'Q2', 'Q3', 'Q4', 'OT']
            expected_stats = ['PTS', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'TOV', 'FOUL', 'REB']
            for period in expected_periods:
                for stat in expected_stats:
                    col_name = f"{period}_{stat}"
                    if col_name not in quarterly_logs.columns:
                        quarterly_logs[col_name] = 0

            # 確保所有欄位排序一致
            cols = ['GAME_ID', 'teamId']
            for p in expected_periods:
                cols.extend([f"{p}_{s}" for s in expected_stats])
            final_quarterly = quarterly_logs[cols]

            final_quarterly.rename(columns={'teamId': 'TEAM_ID'}).to_sql(table, conn, if_exists='append', index=False)
            print(f"   ✔️ 成功寫入 {len(final_quarterly)} 筆。")
        else:
            print(f"✅ [5/5] {table} 已是最新。")

    finally:
        conn.close()
        print("\n🔒 資料庫連線已安全關閉。")

    end_time = time.time()
    print(f"🎉 特徵工廠執行完畢！總耗時: {end_time - start_time:.2f} 秒")

if __name__ == "__main__":
    build_all_pbp_features_to_db()