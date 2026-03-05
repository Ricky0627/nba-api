import csv
import os

# --- 設定區塊 ---
# 定義檔案路徑 (使用 raw string r"..." 避免路徑反斜線轉義問題)
SOURCE_CSV = r"C:\Users\ricky\OneDrive\桌面\nba_new_project\data\historical_play_by_play.csv"
HISTORICAL_CSV = r"C:\Users\ricky\OneDrive\桌面\nba_new_project\data\historical_play_by_play_old.csv"
CURRENT_CSV = r"C:\Users\ricky\OneDrive\桌面\nba-api\data\current_play_by_play.csv"

# 定義新賽季的 GAME_ID 前綴
# 涵蓋你提到的 0025，以及標準的 25-26 賽季常規賽 (00225) 與季後賽 (00425)
NEW_SEASON_PREFIXES = ('0025', '00225', '00425')

def setup_directories():
    """確保目標資料夾存在"""
    os.makedirs(os.path.dirname(HISTORICAL_CSV), exist_ok=True)
    os.makedirs(os.path.dirname(CURRENT_CSV), exist_ok=True)

def split_play_by_play_csv():
    setup_directories()

    print("開始分割 Play-by-Play CSV 檔案...")
    
    # 計算處理的行數，方便追蹤進度
    hist_count = 0
    curr_count = 0

    # 使用 utf-8-sig 可以處理帶有 BOM 的 UTF-8 檔案，避免首個欄位名稱產生亂碼
    with open(SOURCE_CSV, mode='r', encoding='utf-8-sig', newline='') as f_in, \
         open(HISTORICAL_CSV, mode='w', encoding='utf-8', newline='') as f_hist, \
         open(CURRENT_CSV, mode='w', encoding='utf-8', newline='') as f_curr:
        
        reader = csv.reader(f_in)
        writer_hist = csv.writer(f_hist)
        writer_curr = csv.writer(f_curr)

        # 處理標題列 (Header)
        try:
            headers = next(reader)
        except StopIteration:
            print("來源檔案為空！")
            return

        writer_hist.writerow(headers)
        writer_curr.writerow(headers)

        # 找出 GAME_ID 在哪一個欄位索引
        try:
            game_id_idx = headers.index('GAME_ID')
        except ValueError:
            print("錯誤：在 CSV 標題中找不到 'GAME_ID' 欄位！")
            return

        # 逐行讀取並根據 GAME_ID 分類寫入
        for row in reader:
            # 確保該行資料長度足夠，避免空行或不完整的資料報錯
            if len(row) > game_id_idx:
                game_id = str(row[game_id_idx]).strip()
                
                # 判斷是否為新賽季
                if game_id.startswith(NEW_SEASON_PREFIXES):
                    writer_curr.writerow(row)
                    curr_count += 1
                else:
                    writer_hist.writerow(row)
                    hist_count += 1

    print("\n分割完成！")
    print(f"✅ 歷史賽季資料已寫入: {HISTORICAL_CSV} (共 {hist_count} 筆)")
    print(f"✅ 當前賽季資料已寫入: {CURRENT_CSV} (共 {curr_count} 筆)")

if __name__ == "__main__":
    split_play_by_play_csv()