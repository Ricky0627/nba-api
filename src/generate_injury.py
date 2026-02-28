import pandas as pd
import numpy as np
from tqdm import tqdm
import os

# 🔥 引入我們剛剛寫好的神級模組：自動下載並在記憶體中合併歷史與最新資料
from prepare_data import get_merged_dataframe

# ==========================================
# ⚙️ 參數設定
# ==========================================
OUTPUT_CSV = 'nba_advanced_injury_features.csv'

# 滾動窗口設定
ROLLING_WINDOW_LONG = 20  # 長期實力 (跨賽季)

# Rust 係數設定 (天數: 係數)
RUST_THRESHOLDS = [
    (30, 0.7),  # > 30 天: 打 7 折
    (7, 0.8),   # 7-30 天: 打 8 折
    (0, 1.0)    # < 7 天: 無影響
]

def get_rust_factor(days_gap):
    if pd.isna(days_gap): return 1.0 
    for limit, factor in RUST_THRESHOLDS:
        if days_gap > limit:
            return factor
    return 1.0

def generate_features():
    print("🚀 [Injury & Rust] 開始生成進階傷病特徵 (雲端 MLOps 合體版)...")

    # ==========================================
    # 1. 讀取球員數據 (Advanced + Base)
    # ==========================================
    print("   -> 1. 從雲端與本機載入並合併完整球員逐場數據...")
    
    # 透過模組獲取合體後的 Advanced Stats
    df_adv_full = get_merged_dataframe("player_stats_advanced")
    df_adv = df_adv_full[df_adv_full['MIN'] > 0][
        ['GAME_ID', 'TEAM_ID', 'PLAYER_ID', 'GAME_DATE', 'MIN',
         'PIE', 'NET_RATING', 'USG_PCT', 'OFF_RATING', 'DEF_RATING']
    ].copy()
    
    # 透過模組獲取合體後的 Base Stats
    df_base_full = get_merged_dataframe("player_stats_base")
    df_base = df_base_full[df_base_full['MIN'] > 0][
        ['GAME_ID', 'PLAYER_ID', 'PLUS_MINUS', 'NBA_FANTASY_PTS']
    ].copy()
    
    # 合併
    df_stats = pd.merge(df_adv, df_base, on=['GAME_ID', 'PLAYER_ID'], how='inner')
    df_stats['GAME_DATE'] = pd.to_datetime(df_stats['GAME_DATE'])
    
    # 排序
    df_stats = df_stats.sort_values(['PLAYER_ID', 'GAME_DATE'])

    # ==========================================
    # 2. 計算 Rust Factor (久疏戰陣係數)
    # ==========================================
    print("   -> 2. 計算 Rust Factor (距離上一場天數)...")
    
    df_stats['prev_game_date'] = df_stats.groupby('PLAYER_ID')['GAME_DATE'].shift(1)
    df_stats['days_since_last'] = (df_stats['GAME_DATE'] - df_stats['prev_game_date']).dt.days
    df_stats['rust_factor'] = df_stats['days_since_last'].apply(get_rust_factor)
    
    rusty_players = df_stats[df_stats['rust_factor'] < 1.0]
    print(f"      (發現 {len(rusty_players)} 人次有 Rust 折扣)")

    # ==========================================
    # 3. 計算滾動平均 (Rolling Stats)
    # ==========================================
    print(f"   -> 3. 計算球員賽前滾動數據 (R{ROLLING_WINDOW_LONG} & R50)...")
    
    metrics = ['PIE', 'NET_RATING', 'USG_PCT', 'PLUS_MINUS', 'NBA_FANTASY_PTS']
    
    # A. 跨賽季 R20 (shift 1 防洩漏)
    rolling_20 = df_stats.groupby('PLAYER_ID')[metrics].apply(
        lambda x: x.shift(1).rolling(window=ROLLING_WINDOW_LONG, min_periods=1).mean()
    ).reset_index(level=0, drop=True)
    
    # B. 長期 R50 (代表穩定實力)
    rolling_50 = df_stats.groupby('PLAYER_ID')[metrics].apply(
        lambda x: x.shift(1).rolling(window=50, min_periods=1).mean()
    ).reset_index(level=0, drop=True)

    # 合併
    df_stats = df_stats.join(rolling_20, rsuffix='_r20')
    df_stats = df_stats.join(rolling_50, rsuffix='_r50') 

    # ==========================================
    # 4. 計算「上場球員」的 Rust 衝擊 (Active Roster Impact)
    # ==========================================
    print("   -> 4. 計算上場陣容的 Rust-Adjusted Production...")
    
    for m in metrics:
        col_name = f'{m}_r20'
        # Rust Adjusted Value
        df_stats[f'rust_adj_{m}'] = df_stats[col_name].fillna(0) * df_stats['rust_factor']
        
    rust_metrics = [f'rust_adj_{m}' for m in metrics]
    
    active_rust_stats = df_stats.groupby(['GAME_ID', 'TEAM_ID'])[rust_metrics].sum().reset_index()
    active_rename = {col: f'active_{col}' for col in rust_metrics}
    active_rust_stats = active_rust_stats.rename(columns=active_rename)
    
    # 確保關聯用的 ID 為小寫 game_id 以匹配 games 表
    active_rust_stats = active_rust_stats.rename(columns={'GAME_ID': 'game_id'})

    # ==========================================
    # 5. 計算「缺席球員」的損失 (Missing Production)
    # ==========================================
    print("   -> 5. 計算缺席球員損失 (Missing Production)...")
    
    # 準備查找表
    lookup_cols = ['PLAYER_ID', 'GAME_DATE'] + [f'{m}_r20' for m in metrics] + [f'{m}_r50' for m in metrics]
    lookup_df = df_stats[lookup_cols].dropna(subset=['GAME_DATE'])
    lookup_df = lookup_df.sort_values('GAME_DATE')
    
    # 讀取缺席表 (從合體模組)
    inactive_full = get_merged_dataframe("inactive_players")
    
    # 🔥 關鍵修復：因為 inactive_players 表的欄位是小寫，所以這裡要用小寫讀取！
    inactive = inactive_full[['game_id', 'team_id', 'player_id']].copy()
    
    # 把 player_id 轉成大寫，好讓後面可以跟歷史數據 (lookup_df) 合併
    inactive = inactive.rename(columns={'player_id': 'PLAYER_ID'})
    
    # 讀取比賽日期 (從合體模組)
    games_full = get_merged_dataframe("games")
    games = games_full[['game_id', 'date']].copy()
    games = games.rename(columns={'date': 'GAME_DATE'})
    games['GAME_DATE'] = pd.to_datetime(games['GAME_DATE'])
    
    # 合併
    inactive = inactive.merge(games, on='game_id', how='left')
    
    before_len = len(inactive)
    inactive = inactive.dropna(subset=['GAME_DATE'])
    after_len = len(inactive)
    if before_len > after_len:
        print(f"      ⚠️ 已過濾 {before_len - after_len} 筆無效日期的缺席紀錄。")
        
    inactive = inactive.sort_values('GAME_DATE')
    
    # 匹配缺席者數據
    merged_inactive = pd.merge_asof(
        inactive,
        lookup_df,
        on='GAME_DATE',
        by='PLAYER_ID',
        direction='backward'
    )
    
    agg_dict = {f'{m}_r20': 'sum' for m in metrics}
    agg_dict.update({f'{m}_r50': 'sum' for m in metrics})
    
    missing_stats = merged_inactive.groupby(['game_id', 'team_id']).agg(agg_dict).reset_index()
    
    missing_rename = {col: f'missing_{col}' for col in agg_dict.keys()}
    missing_stats = missing_stats.rename(columns=missing_rename)

    # ==========================================
    # 6. 合併所有特徵並輸出
    # ==========================================
    print("   -> 6. 合併主客隊特徵與輸出 CSV...")
    
    games_final = games_full[['game_id', 'home_team', 'away_team', 'date']].copy()
    
    # Team Mapping
    teams_map = df_base_full[['TEAM_ID', 'TEAM_ABBREVIATION']].drop_duplicates()
    
    # 處理 Active Rust
    active_rust_stats = active_rust_stats.merge(teams_map, on='TEAM_ID', how='left')
    
    # 處理 Missing Stats
    missing_stats = missing_stats.merge(teams_map, left_on='team_id', right_on='TEAM_ID', how='left')
    
    # 定義合併函數
    def merge_side_features(df, feat_df, side, prefix):
        if 'GAME_ID' in feat_df.columns: 
            feat_df = feat_df.rename(columns={'GAME_ID': 'game_id'})
            
        df = df.merge(
            feat_df,
            left_on=['game_id', side],
            right_on=['game_id', 'TEAM_ABBREVIATION'],
            how='left'
        )
        
        cols_to_rename = [c for c in feat_df.columns if c not in ['game_id', 'TEAM_ID', 'TEAM_ABBREVIATION', 'team_id']]
        rename_map = {c: f'{prefix}_{c}' for c in cols_to_rename}
        df = df.rename(columns=rename_map)
        
        drop_cols = ['TEAM_ID', 'TEAM_ABBREVIATION', 'team_id']
        df = df.drop(columns=[c for c in drop_cols if c in df.columns])
        
        return df, cols_to_rename

    # 合併
    games_final, active_cols = merge_side_features(games_final, active_rust_stats, 'home_team', 'home')
    games_final, _ = merge_side_features(games_final, active_rust_stats, 'away_team', 'away')
    
    games_final, missing_cols = merge_side_features(games_final, missing_stats, 'home_team', 'home')
    games_final, _ = merge_side_features(games_final, missing_stats, 'away_team', 'away')
    
    # 補 0
    numeric_cols = [c for c in games_final.columns if 'active_' in c or 'missing_' in c]
    games_final[numeric_cols] = games_final[numeric_cols].fillna(0)
    
    # 計算差值 (主 - 客)
    base_feats = active_cols + missing_cols
    for feat in base_feats:
        games_final[f'diff_{feat}'] = games_final[f'home_{feat}'] - games_final[f'away_{feat}']

    # 輸出
    games_final.to_csv(OUTPUT_CSV, index=False)
    print(f"\n✅ 成功匯出: {OUTPUT_CSV}")
    print(f"   總共生成 {len(games_final.columns)} 個欄位")

if __name__ == "__main__":
    generate_features()
