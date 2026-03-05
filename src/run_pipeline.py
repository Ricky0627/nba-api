import subprocess
import time
import sys

# 定義每日更新的標準執行順序
PIPELINE_SCRIPTS = [
    # 階段一：獲取最新基礎數據
    ("獲取球隊基礎數據", "src/fetch_data.py"),
    ("獲取球隊擴充數據", "src/fetch_extended_stats.py"),
    ("獲取球員個人數據", "src/fetch_player_stats.py"),
    
    # 階段二：建構主賽程表 (後續腳本高度依賴此表)
    ("同步主賽程表", "src/init_games_table.py"),
    
    # 階段三：補充外部與深度資訊
    ("獲取傷兵名單", "src/fetch_inactive_players.py"),
    ("獲取運彩賠率", "src/fetch_odds.py"),
    ("獲取拼搶數據 (Hustle)", "src/fetch_hustle.py"),           # 👈 新增：Hustle 數據
    ("獲取逐球轉播 (Play-by-Play)", "src/fetch_pbp.py"),     # 👈 新增：PBP 數據
    
    # 階段四：特徵工程 (Feature Engineering)
    ("提煉 PBP 五大進階特徵", "src/build_all_pbp_features_to_db.py"),  # 👈 新增：PBP 特徵提煉
    ("生成進階傷病與滾動特徵", "src/generate_injury.py"),
    
    # 階段五：模型回測與部署
    ("執行增量回測與結算", "src/nba_daily_backtest.py"),
    ("重新訓練並部署模型", "src/train_deploy.py"),
    
    # 階段六：後續任務
    ("更新賽事資訊", "src/fetch_games.py")
]

def run_script(description, script_path):
    print(f"\n{'='*60}")
    print(f"▶️ 開始執行: {description} ({script_path})")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    try:
        # 使用 sys.executable 確保使用目前的 Python 環境
        result = subprocess.run([sys.executable, script_path], check=True)
        elapsed_time = time.time() - start_time
        print(f"\n✅ [{description}] 執行成功！ (耗時: {elapsed_time:.1f} 秒)")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ [{description}] 執行失敗！錯誤碼: {e.returncode}")
        return False
    except FileNotFoundError:
        print(f"\n❌ 找不到檔案: {script_path}，請檢查路徑是否正確。")
        return False

def main():
    print("🌟 NBA 每日 AI 預測系統 - 全自動更新管線啟動 🌟")
    total_start = time.time()
    
    for desc, script in PIPELINE_SCRIPTS:
        success = run_script(desc, script)
        if not success:
            print("\n⚠️ 管線已中斷。請先修復上述錯誤後再重新執行。")
            sys.exit(1)
            
        time.sleep(2) # 每個腳本之間稍微緩衝一下
        
    total_elapsed = (time.time() - total_start) / 60
    print(f"\n🎉 恭喜！所有更新任務皆已順利完成！ (總耗時: {total_elapsed:.1f} 分鐘)")
    print("👉 現在你可以使用最新訓練好的模型進行今日賽事預測了。")

if __name__ == "__main__":
    main()