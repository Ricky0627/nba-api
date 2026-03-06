import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import warnings
from datetime import datetime, timedelta
from pytz import timezone
import sqlite3
import time
import random
import re
import json

# 隱藏 V2 的警告
warnings.filterwarnings('ignore', category=DeprecationWarning)
from nba_api.stats.endpoints import scoreboardv2

# ==========================================
# ⚙️ 雲端自動化設定區
# ==========================================
DB_PATH = 'data/nba_current.db'

# 專門用來動態開關 Proxy 的函數 (優先讀取 PROXY_URL2)
def toggle_proxy(enable: bool):
    proxy_url = os.environ.get('PROXY_URL2') or os.environ.get('PROXY_URL')
    if enable and proxy_url:
        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url
    else:
        os.environ.pop('HTTP_PROXY', None)
        os.environ.pop('HTTPS_PROXY', None)

def get_db_connection():
    db_path = DB_PATH
    if not os.path.exists(db_path) and os.path.exists('../data/nba_current.db'):
        db_path = '../data/nba_current.db'
    elif not os.path.exists(db_path) and os.path.exists('../data/nba_raw.db'):
        db_path = '../data/nba_raw.db'
    return sqlite3.connect(db_path, timeout=15.0)

team_id_to_abbr = {
    1610612737: 'ATL', 1610612738: 'BOS', 1610612751: 'BKN', 1610612766: 'CHA', 1610612741: 'CHI',
    1610612739: 'CLE', 1610612742: 'DAL', 1610612743: 'DEN', 1610612765: 'DET', 1610612744: 'GSW',
    1610612745: 'HOU', 1610612754: 'IND', 1610612746: 'LAC', 1610612747: 'LAL', 1610612763: 'MEM',
    1610612748: 'MIA', 1610612749: 'MIL', 1610612750: 'MIN', 1610612740: 'NOP', 1610612752: 'NYK',
    1610612760: 'OKC', 1610612753: 'ORL', 1610612755: 'PHI', 1610612756: 'PHX', 1610612757: 'POR',
    1610612758: 'SAC', 1610612759: 'SAS', 1610612761: 'TOR', 1610612762: 'UTA', 1610612764: 'WAS'
}

def get_recent_roster(team_abbr: str):
    try:
        conn = get_db_connection()
        query = f"""
            SELECT DISTINCT PLAYER_ID, PLAYER_NAME
            FROM player_stats_base
            WHERE TEAM_ABBREVIATION = '{team_abbr}' 
            AND GAME_ID IN (
                SELECT GAME_ID FROM boxscore_base 
                WHERE TEAM_ABBREVIATION = '{team_abbr}' 
                ORDER BY GAME_DATE DESC LIMIT 10
            )
            AND MIN IS NOT NULL AND MIN != '0' AND MIN != '0:00'
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return [{"id": row['PLAYER_ID'], "name": row['PLAYER_NAME']} for _, row in df.iterrows()]
    except Exception as e:
        print(f"⚠️ 讀取陣容失敗: {e}")
        return []

def parse_cell_robust(text):
    if not text or text == '-' or '未開' in text: return None, None
    is_pk = 'PK' in text.upper()
    text = re.sub(r'\(.*?\)', '', text).replace('&nbsp;', '').strip()
    nums = re.findall(r'[-+]?\d+\.\d+|[-+]?\d+', text)
    final_val, final_odds = None, None
    if not nums: return (0.0, None) if is_pk else (None, None)
    nums_float = []
    for n in nums:
        try: nums_float.append(float(n))
        except: pass
    if not nums_float: return None, None
    if len(nums_float) == 1:
        val = nums_float[0]
        if val > 50 or val == 0: final_val = val
        else: final_odds = val 
    elif len(nums_float) >= 2:
        final_odds, final_val = nums_float[-1], nums_float[-2]
    if is_pk: final_val = 0.0
    return final_val, final_odds

def parse_tot_smart(txt):
    if not txt: return None
    v, o = parse_cell_robust(txt)
    if v is not None: v = abs(v)
    return v

def scrape_playsport_odds(target_date_tw_str):
    url = f"https://www.playsport.cc/gamesData/result?allianceid=3&gametime={target_date_tw_str}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    odds_dict = {}
    TEAM_MAPPING = {"湖人": "LAL", "勇士": "GSW", "金塊": "DEN", "塞爾提": "BOS", "公鹿": "MIL", "七六人": "PHI", "76人": "PHI", "太陽": "PHX", "快艇": "LAC", "熱火": "MIA", "尼克": "NYK", "騎士": "CLE", "獨行俠": "DAL", "小牛": "DAL", "灰熊": "MEM", "國王": "SAC", "老鷹": "ATL", "溜馬": "IND", "暴龍": "TOR", "公牛": "CHI", "雷霆": "OKC", "灰狼": "MIN", "爵士": "UTA", "拓荒者": "POR", "魔術": "ORL", "巫師": "WAS", "火箭": "HOU", "馬刺": "SAS", "活塞": "DET", "籃網": "BKN", "鵜鶘": "NOP", "黃蜂": "CHA"}
    try:
        toggle_proxy(False) # 爬運彩不需要 Proxy，避免被擋
        r = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(r.content, 'html.parser')
        rows = soup.find_all('tr', attrs={'gameid': True})
        i = 0
        while i < len(rows) - 1:
            r_away, r_home = rows[i], rows[i+1]
            if r_away.get('gameid') != r_home.get('gameid'): i += 1; continue
            
            td_a, td_h = r_away.find('td', class_='td-teaminfo'), r_home.find('td', class_='td-teaminfo')
            if not td_a or not td_h: i += 2; continue
            
            t_name_a = td_a.find('a').text.strip() if td_a.find('a') else ""
            t_name_h = td_h.find('a').text.strip() if td_h.find('a') else ""
            code_away = next((v for k, v in TEAM_MAPPING.items() if k in t_name_a), None)
            code_home = next((v for k, v in TEAM_MAPPING.items() if k in t_name_h), None)
            
            if code_away and code_home:
                t_spr_a = r_away.find('td', 'td-bank-bet01').get_text(separator=' ').strip() if r_away.find('td', 'td-bank-bet01') else ""
                t_spr_h = r_home.find('td', 'td-bank-bet01').get_text(separator=' ').strip() if r_home.find('td', 'td-bank-bet01') else ""
                val_a, _ = parse_cell_robust(t_spr_a)
                val_h, _ = parse_cell_robust(t_spr_h)
                spread_val = val_h if val_h is not None else (-val_a if val_a is not None else None)
                
                t_tot_a = r_away.find('td', 'td-bank-bet02').get_text(separator=' ').strip() if r_away.find('td', 'td-bank-bet02') else ""
                t_tot_h = r_home.find('td', 'td-bank-bet02').get_text(separator=' ').strip() if r_home.find('td', 'td-bank-bet02') else ""
                tot_a = parse_tot_smart(t_tot_a)
                tot_h = parse_tot_smart(t_tot_h)
                total_val = tot_a if tot_a and tot_a > 100 else (tot_h if tot_h and tot_h > 100 else None)

                if spread_val is not None or total_val is not None:
                    odds_dict[code_home] = {"spread": spread_val, "total": total_val}
            i += 2
    except: pass
    return odds_dict

# 🔥 超強偽裝 Header
def get_random_header():
    return {
        'Host': 'stats.nba.com',
        'User-Agent': random.choice([
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.nba.com/',
        'Origin': 'https://www.nba.com',
    }

def get_b2b_teams(us_date_str):
    try:
        yday_str = (datetime.strptime(us_date_str, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        board = scoreboardv2.ScoreboardV2(game_date=yday_str, headers=get_random_header(), timeout=25)
        df = board.game_header.get_data_frame()
        b2b_teams = []
        for _, row in df.iterrows():
            b2b_teams.append(team_id_to_abbr.get(row['HOME_TEAM_ID']))
            b2b_teams.append(team_id_to_abbr.get(row['VISITOR_TEAM_ID']))
        return [t for t in b2b_teams if t]
    except: return []

def scrape_espn_injuries():
    url = "https://www.espn.com/nba/injuries"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive'
    }
    exact_mapping = {
        "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN", "Charlotte Hornets": "CHA",
        "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE", "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN",
        "Detroit Pistons": "DET", "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
        "LA Clippers": "LAC", "Los Angeles Clippers": "LAC", "Los Angeles Lakers": "LAL", "LA Lakers": "LAL",
        "Memphis Grizzlies": "MEM", "Miami Heat": "MIA", "Milwaukee Bucks": "MIL", "Minnesota Timberwolves": "MIN",
        "New Orleans Pelicans": "NOP", "New York Knicks": "NYK", "Oklahoma City Thunder": "OKC", "Orlando Magic": "ORL",
        "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX", "Portland Trail Blazers": "POR", "Sacramento Kings": "SAC",
        "San Antonio Spurs": "SAS", "Toronto Raptors": "TOR", "Utah Jazz": "UTA", "Washington Wizards": "WAS"
    }
    injuries = {abbr: [] for abbr in set(exact_mapping.values())}
    try:
        toggle_proxy(True) # 找回你的 Proxy 來抓 ESPN
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code != 200: return injuries
        match = re.search(r'"injuries":(\[\{"displayName.*?\]\}\])', r.text)
        if not match: return injuries
        teams_data = json.loads(match.group(1))
        for team in teams_data:
            abbr = exact_mapping.get(team.get("displayName", "").strip())
            if not abbr: continue
            for item in team.get("items", []):
                name = item.get("athlete", {}).get("name", "Unknown")
                status = item.get("statusDesc", "Unknown")
                comment = item.get("description", "")
                if name and status:
                    injuries[abbr].append({"name": name, "status": status, "comment": comment})
    except: pass
    return injuries

def fetch_and_save_upcoming_games():
    print("🚀 開始抓取明日/近期賽程...")
    tw_tz = timezone('Asia/Taipei')
    us_tz = timezone('US/Eastern')
    start_date = datetime.now(tw_tz) - timedelta(days=1)
    espn_injuries = scrape_espn_injuries()
    upcoming_games = []
    
    for i in range(7):
        target_date = start_date + timedelta(days=i)
        us_date_str = target_date.astimezone(us_tz).strftime('%Y-%m-%d')
        tw_date_str = (datetime.strptime(us_date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y%m%d')
        
        for attempt in range(3):
            try:
                print(f"📡 正在檢查日期 {us_date_str} (嘗試次數 {attempt+1}/3)...")
                
                if attempt < 2:
                    toggle_proxy(True)
                else:
                    toggle_proxy(False)
                    print(f"   🔄 第 3 次嘗試切換為「無 Proxy 直連」...")
                
                time.sleep(random.uniform(1.5, 3.5))
                board = scoreboardv2.ScoreboardV2(game_date=us_date_str, headers=get_random_header(), timeout=25)
                games = board.game_header.get_data_frame()
                
                if games.empty: 
                    print(f"   ⏩ {us_date_str} 沒有比賽資料")
                    break
                
                unplayed_games = games[~games['GAME_STATUS_TEXT'].str.contains('Final', case=False, na=False)]
                unplayed_games = unplayed_games.drop_duplicates(subset=['GAME_ID'], keep='first')
                
                if unplayed_games.empty: 
                    print(f"   ⏩ {us_date_str} 的比賽都打完了")
                    break
                    
                print(f"   ✅ 找到 {len(unplayed_games)} 場未開打賽事，開始整理資料...")
                yday_teams = get_b2b_teams(us_date_str)
                todays_odds = scrape_playsport_odds(tw_date_str)
                
                for _, row in unplayed_games.iterrows():
                    game_id = row['GAME_ID']
                    home_abbr = team_id_to_abbr.get(row['HOME_TEAM_ID'])
                    away_abbr = team_id_to_abbr.get(row['VISITOR_TEAM_ID'])
                    if not home_abbr or not away_abbr: continue
                    
                    home_roster = get_recent_roster(home_abbr)
                    away_roster = get_recent_roster(away_abbr)
                    
                    def match_injuries(team_abbr, roster):
                        matched = []
                        for inj in espn_injuries.get(team_abbr, []):
                            for player in roster:
                                clean_inj_name = inj["name"].replace('Jr.', '').replace('Sr.', '').replace('III', '').strip()
                                last_name = clean_inj_name.split()[-1]
                                if last_name in player['name']: 
                                    matched.append({"id": player['id'], "name": player['name'], "status": inj["status"], "comment": inj["comment"]})
                                    break
                        return json.dumps(matched, ensure_ascii=False)

                    game_odds = todays_odds.get(home_abbr, {"spread": 0.0, "total": 0.0})
                    
                    upcoming_games.append({
                        "game_date": us_date_str, "game_id": str(game_id).zfill(10),
                        "home_team": home_abbr, "away_team": away_abbr,
                        "status": row['GAME_STATUS_TEXT'], 
                        "vegas_spread": game_odds.get("spread", 0.0), "vegas_total": game_odds.get("total", 0.0),
                        "home_is_b2b": home_abbr in yday_teams, "away_is_b2b": away_abbr in yday_teams,
                        "home_injuries_ids": match_injuries(home_abbr, home_roster), 
                        "away_injuries_ids": match_injuries(away_abbr, away_roster)
                    })
                break
            except Exception as e:
                err_msg = str(e).split("Caused by")[-1] if "Caused by" in str(e) else str(e)
                print(f"   ⚠️ 第 {attempt+1} 次連線失敗: {err_msg}")
                time.sleep(3) 
                
        if len(upcoming_games) > 0: break

    if upcoming_games:
        os.makedirs("data", exist_ok=True)
        df = pd.DataFrame(upcoming_games)
        df.to_csv("data/upcoming_games.csv", index=False, encoding="utf-8-sig")
        print(f"\n🎉 大功告成！成功儲存 {len(df)} 場賽程至 data/upcoming_games.csv！")
    else: 
        print("\n🤷‍♂️ 掃描完畢，未來 7 天內沒有找到任何未開打的賽事。")

if __name__ == "__main__":
    fetch_and_save_upcoming_games()