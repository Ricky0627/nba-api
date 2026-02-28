import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import random
import datetime
from datetime import timedelta
import re
import os

# ===========================
# ⚙️ 雲端自動化設定區
# ===========================
DB_PATH = 'data/nba_current.db'    # 👈 改為讀寫輕量級的新資料庫
DEFAULT_START_DATE = "2025-10-15"  # 👈 雲端版只負責 2025-26 當前賽季
DEFAULT_END_DATE   = "2026-06-30"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# ===========================
# 🛡️ Proxy 代理伺服器設定
# ===========================
def setup_proxy():
    """從 GitHub Secrets 讀取專屬 Proxy 並設定為全域環境變數"""
    proxy_url = os.environ.get('PROXY_URL')
    if proxy_url:
        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url
        print("✅ 已成功載入 Webshare 私人 Proxy 設定！")
    else:
        print("⚠️ 警告：未偵測到 PROXY_URL 環境變數，將使用 GitHub 預設 IP 連線（極可能被擋）。")

# 🏀 隊名對照表 (整合版)
TEAM_MAPPING = {
    "湖人": "LAL", "洛杉磯湖人": "LAL", "勇士": "GSW", "金州勇士": "GSW", "金塊": "DEN", "丹佛金塊": "DEN",
    "塞爾提克": "BOS", "波士頓塞爾提克": "BOS", "塞爾提": "BOS", "公鹿": "MIL", "密爾瓦基公鹿": "MIL",
    "七六人": "PHI", "費城七六人": "PHI", "76人": "PHI", "太陽": "PHX", "鳳凰城太陽": "PHX", "鳳凰城": "PHX",
    "快艇": "LAC", "洛杉磯快艇": "LAC", "熱火": "MIA", "邁阿密熱火": "MIA", "尼克": "NYK", "紐約尼克": "NYK",
    "騎士": "CLE", "克里夫蘭騎士": "CLE", "獨行俠": "DAL", "達拉斯獨行俠": "DAL", "小牛": "DAL", "達拉斯小牛": "DAL",
    "灰熊": "MEM", "曼菲斯灰熊": "MEM", "國王": "SAC", "沙加緬度國王": "SAC", "老鷹": "ATL", "亞特蘭大老鷹": "ATL",
    "溜馬": "IND", "印第安那溜馬": "IND", "暴龍": "TOR", "多倫多暴龍": "TOR", "公牛": "CHI", "芝加哥公牛": "CHI",
    "雷霆": "OKC", "奧克拉荷馬雷霆": "OKC", "灰狼": "MIN", "明尼蘇達灰狼": "MIN", "爵士": "UTA", "猶他爵士": "UTA",
    "拓荒者": "POR", "波特蘭拓荒者": "POR", "拓荒": "POR", "魔術": "ORL", "奧蘭多魔術": "ORL",
    "巫師": "WAS", "華盛頓巫師": "WAS", "火箭": "HOU", "休士頓火箭": "HOU", "馬刺": "SAS", "聖安東尼奧馬刺": "SAS",
    "活塞": "DET", "底特律活塞": "DET", "籃網": "BKN", "布魯克林籃網": "BKN", "紐澤西籃網": "BKN", "紐澤西": "BKN", 
    "鵜鶘": "NOP", "紐奧良鵜鶘": "NOP", "紐奧良黃蜂": "NOP", "紐奧良": "NOP", 
    "黃蜂": "CHA", "夏洛特黃蜂": "CHA", "山貓": "CHA", "夏洛特山貓": "CHA", 
}

# 隊名別名，用於容錯匹配
CODE_ALIASES = {
    "BKN": ["BRK", "NJN"], "BRK": ["BKN"], "NJN": ["BKN"],
    "NOP": ["NOH", "NOK"], "NOH": ["NOP"], "CHA": ["CHO", "CHH"], "CHO": ["CHA"],
    "PHX": ["PHO"], "PHO": ["PHX"], "WAS": ["WSB"]
}

def get_db_connection():
    return sqlite3.connect(DB_PATH, timeout=30.0)

def date_range(start, end):
    """產生日期序列，並處理 T00:00:00 問題"""
    if 'T' in start: start = start.split('T')[0]
    if 'T' in end: end = end.split('T')[0]
    
    s = datetime.datetime.strptime(start, "%Y-%m-%d")
    e = datetime.datetime.strptime(end, "%Y-%m-%d")
    for i in range((e - s).days + 1):
        yield s + timedelta(days=i)

def find_game_in_db(date_str, h_code, a_code):
    """
    在資料庫中尋找對應的 game_id
    date_str: YYYYMMDD (台灣時間)
    考慮時差，嘗試 台灣日期 -1, 0, -2 天 (NBA 比賽通常是台灣時間的昨天或當天)
    """
    conn = get_db_connection(); c = conn.cursor()
    dt = datetime.datetime.strptime(date_str, "%Y%m%d")
    
    # 擴展主客隊代碼 (處理球隊改名或縮寫不同)
    h_list = [h_code] + CODE_ALIASES.get(h_code, [])
    a_list = [a_code] + CODE_ALIASES.get(a_code, [])
    
    # 搜尋視窗：T-1 (最常見), T (同天), T-2 (極少見)
    for diff in [1, 0, 2]: 
        t_date = (dt - timedelta(days=diff)).strftime("%Y-%m-%d")
        
        # 模糊搜尋日期 (忽略時間部分)
        date_pattern = f"{t_date}%"
        
        for h in h_list:
            for a in a_list:
                c.execute('SELECT game_id FROM games WHERE home_team=? AND away_team=? AND date LIKE ?', (h, a, date_pattern))
                res = c.fetchone()
                if res: 
                    conn.close()
                    return res[0]
                    
    conn.close()
    return None

def parse_cell_robust(text):
    """
    🔥 核心引擎 V5 (修復版)：
    解決完賽後「賠率黏著輸贏字眼」(如 "1.75贏50%") 導致抓不到資料的問題。
    """
    if not text or text == '-' or '未開' in text: return None, None
    
    # 1. 預處理：檢查 PK
    is_pk = 'PK' in text.upper()
        
    # 2. 清洗資料：移除括號內的詳情，以及「贏/輸」後面的百分比
    text = re.sub(r'\(.*?\)', '', text).replace('&nbsp;', '').strip()
    
    # 3. 使用 Regex 強制提取所有數字 (支援負號與小數點)
    nums = re.findall(r'[-+]?\d+\.\d+|[-+]?\d+', text)
    
    final_val = None
    final_odds = None
    
    if not nums:
        if is_pk: return 0.0, None
        return None, None

    # 轉成 float 列表
    nums_float = []
    for n in nums:
        try: nums_float.append(float(n))
        except: pass

    if not nums_float: return None, None

    # 4. 智慧分配邏輯
    if len(nums_float) == 1:
        val = nums_float[0]
        if val > 50 or val == 0: # 大小分分數或PK
            final_val = val
        else:
            final_odds = val # 賠率
            
    elif len(nums_float) >= 2:
        final_odds = nums_float[-1] # 最後一個是賠率
        final_val = nums_float[-2]  # 倒數第二個是分數

    if is_pk: final_val = 0.0

    return final_val, final_odds

def parse_tot_smart(txt):
    """解析大小分"""
    if not txt: return None, None, False
    is_ov = '大' in txt
    v, o = parse_cell_robust(txt)
    if v is not None: v = abs(v) # 大小分一定是正數
    return v, o, is_ov

def update_db(game_id, data):
    """更新資料庫"""
    conn = get_db_connection()
    c = conn.cursor()
    cols = []
    vals = []
    for k, v in data.items():
        cols.append(f"{k}=?")
        vals.append(v)
    vals.append(game_id)
    if cols:
        sql = f"UPDATE games SET {', '.join(cols)} WHERE game_id=?"
        try: c.execute(sql, vals); conn.commit()
        except: pass
    conn.close()

def get_db_date_range():
    """找出資料庫中最晚的賠率日期，作為下次爬蟲的起點"""
    if not os.path.exists(DB_PATH):
        return DEFAULT_START_DATE, DEFAULT_END_DATE

    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # 檢查是否已有賠率數據
        c.execute("SELECT MAX(date) FROM games WHERE tw_spread_score IS NOT NULL")
        last_odds_date = c.fetchone()[0]
        
        # 今天的日期
        today = datetime.datetime.now().strftime("%Y-%m-%d")

        if last_odds_date:
            print(f"   ℹ️ 資料庫已有賠率至 {last_odds_date}，將從隔天開始爬取。")
            if 'T' in last_odds_date: last_odds_date = last_odds_date.split('T')[0]
            
            last_dt = datetime.datetime.strptime(last_odds_date, "%Y-%m-%d")
            next_day = (last_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            return next_day, DEFAULT_END_DATE
        else:
            # 如果沒賠率，找最早的比賽日期
            c.execute("SELECT MIN(date) FROM games")
            first_game_date = c.fetchone()[0]
            if first_game_date:
                if 'T' in first_game_date: first_game_date = first_game_date.split('T')[0]
                print(f"   ℹ️ 尚未有任何賠率資料。將從資料庫最早日期 {first_game_date} 開始爬取。")
                return first_game_date, DEFAULT_END_DATE
            else:
                return DEFAULT_START_DATE, DEFAULT_END_DATE
            
    except Exception as e:
        print(f"   ⚠️ 讀取日期範圍錯誤: {e}")
        return DEFAULT_START_DATE, DEFAULT_END_DATE
    finally:
        conn.close()

def crawl_odds_incremental():
    print("🔍 正在檢查資料庫進度...")
    start_date, end_date = get_db_date_range()
    
    # 防止爬取未來 (PlaySport 最多只會有未來幾天的盤)
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    limit_date = (datetime.datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d") # 最多爬到未來一週
    
    if start_date > limit_date:
        print("✅ 賠率資料已是最新，無需更新。")
        return

    print(f"🚀 PlaySport 運彩盤爬蟲 (雲端自動化版) 啟動...")
    print(f"📅 範圍: {start_date} ~ {min(end_date, limit_date)}")
    
    for curr in date_range(start_date, min(end_date, limit_date)):
        date_str = curr.strftime("%Y%m%d") # 網址用 YYYYMMDD
        display = curr.strftime("%Y-%m-%d")
        
        # 🔥 使用更穩定的歷史賽果網址
        url = f"https://www.playsport.cc/gamesData/result?allianceid=3&gametime={date_str}"
        print(f"   📥 正在爬取 {display} ... ", end="")
        
        try:
            # 由於已設定 os.environ['HTTPS_PROXY']，這裡的 requests 會自動走 Webshare 代理
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200: 
                print(f"失敗 ({resp.status_code})")
                continue
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            rows = soup.find_all('tr', attrs={'gameid': True})
            
            count = 0
            i = 0
            while i < len(rows) - 1:
                r_away = rows[i]
                r_home = rows[i+1]
                
                if r_away.get('gameid') != r_home.get('gameid'):
                    i += 1; continue
                    
                td_info = r_away.find('td', class_='td-teaminfo')
                if not td_info: i += 2; continue
                
                teams = [l.text.strip() for l in td_info.find_all('a') if 'teamid=' in l.get('href', '')]
                if len(teams) < 2: i += 2; continue
                
                c_away, c_home = teams[0], teams[1]
                code_away = TEAM_MAPPING.get(c_away)
                code_home = TEAM_MAPPING.get(c_home)
                
                if not code_away or not code_home: i += 2; continue
                
                gid = find_game_in_db(date_str, code_home, code_away)
                if not gid: 
                    i += 2; continue
                
                data = {}
                
                # 1. 運彩讓分 (Spread)
                try:
                    t_spr_a = r_away.find('td', 'td-bank-bet01').get_text(separator=',').strip()
                    t_spr_h = r_home.find('td', 'td-bank-bet01').get_text(separator=',').strip()
                    _, o_spr_a = parse_cell_robust(t_spr_a)
                    s_spr, o_spr_h = parse_cell_robust(t_spr_h)
                    
                    if s_spr is not None: data['tw_spread_score'] = s_spr
                    if o_spr_h: data['tw_spread_home_odds'] = o_spr_h
                    if o_spr_a: data['tw_spread_away_odds'] = o_spr_a
                except: pass
                
                # 2. 運彩大小 (Total)
                try:
                    t_tot_a = r_away.find('td', 'td-bank-bet02').get_text(separator=',').strip()
                    t_tot_h = r_home.find('td', 'td-bank-bet02').get_text(separator=',').strip()
                    v1, o1, is_ov1 = parse_tot_smart(t_tot_a)
                    v2, o2, is_ov2 = parse_tot_smart(t_tot_h)
                    
                    final_tot = v1 if v1 and v1 > 100 else (v2 if v2 and v2 > 100 else None)
                    if final_tot: data['tw_total_score'] = final_tot
                    
                    if o1: data['tw_total_over_odds' if is_ov1 else 'tw_total_under_odds'] = o1
                    if o2: data['tw_total_over_odds' if is_ov2 else 'tw_total_under_odds'] = o2
                except: pass

                # 3. 運彩獨贏 (Moneyline)
                try:
                    t_ml_a = r_away.find('td', 'td-bank-bet03').get_text(separator=',').strip()
                    t_ml_h = r_home.find('td', 'td-bank-bet03').get_text(separator=',').strip()
                    _, o_ml_a = parse_cell_robust(t_ml_a)
                    _, o_ml_h = parse_cell_robust(t_ml_h)
                    if o_ml_a: data['tw_moneyline_away'] = o_ml_a
                    if o_ml_h: data['tw_moneyline_home'] = o_ml_h
                except: pass
                
                if data:
                    update_db(gid, data)
                    count += 1
                i += 2
            
            print(f"更新 {count} 場")
            # 禮貌性延遲
            time.sleep(random.uniform(1.0, 2.0))
            
        except Exception as e:
            print(f"錯誤: {e}")
            pass

if __name__ == "__main__":
    print(f"🚀 啟動 NBA 運彩賠率爬蟲 (雲端全自動更新版)")
    # 初始化 Proxy
    setup_proxy()
    
    if not os.path.exists(DB_PATH):
        print(f"❌ 找不到資料庫 {DB_PATH}，請先執行 init_games_table.py")
    else:
        crawl_odds_incremental()