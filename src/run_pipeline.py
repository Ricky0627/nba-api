import subprocess
import time
import sys
import os

# 定義每日更新的標準執行順序
PIPELINE_SCRIPTS = [
    ("獲取球隊基礎數據", "src/fetch_data.py"),
    ("獲取球隊擴充數據", "src/fetch_extended_stats.py"),
    ("獲取球員個人數據", "src/fetch_player_stats.py"),
    ("同步主賽程表", "src/init_games_table.py"),
    ("獲取傷兵名單", "src/fetch_inactive_players.py"),
    ("獲取運彩賠率", "src/fetch_odds.py"),
    ("生成進階傷病特徵", "src/generate_injury.py"), # 👈 剛才卡在這裡
    ("執行增量回測與結算", "src/nba_daily_backtest.py"),
    ("重新訓練並部署模型", "src/train_deploy.py")
]

def run_script(description, script_path):
    print(f"\n{'='*60}")
    print(f"▶️ 開始執行: {description} ({script_path})")
    print(f"{'='*60}")
    
    # 🔥 關鍵修復：將 src 加入 PYTHONPATH 環境變數
    # 這樣在 src/ 裡面的檔案才能互相 import
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.abspath("src") + os.pathsep + env.get("PYTHONPATH", "")

    try:
        # 傳入 env=env
        result = subprocess.run([sys.executable, script_path], check=True, env=env)
        print(f"\n✅ [{description}] 執行成功！")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ [{description}] 執行失敗！錯誤碼: {e.returncode}")
        return False
    except FileNotFoundError:
        print(f"\n❌ 找不到檔案: {script_path}")
        return False

def main():
    print("🌟 NBA 每日 AI 預測系統 - 全自動更新管線啟動 🌟")
    for desc, script in PIPELINE_SCRIPTS:
        success = run_script(desc, script)
        if not success:
            print("\n⚠️ 管線已中斷。")
            sys.exit(1)
        time.sleep(2)
    print(f"\n🎉 恭喜！所有更新任務皆已順利完成！")

if __name__ == "__main__":
    main()
