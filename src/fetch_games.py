import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pytz import timezone
import sqlite3
import json
import re

# ==========================================
# ⚙️ 雲端自動化設定區
# ==========================================
DB_PATH = 'data/nba_current.db'

# 動態開關 Proxy (優先讀取 PROXY_URL2)
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
    except Exception:
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
        toggle_proxy(False) # 爬運彩不需要 Proxy
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

def scrape_espn_injuries():
    url = "https://www.espn.com/nba/injuries"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
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
    
    r = None
    try:
        toggle_proxy(True) # 嘗試用 Proxy 抓 ESPN
        r = requests.get(url, headers=headers, timeout=10)
    except:
        print("   🔄 Proxy 連線 ESPN 失敗，自動切換為直連...")
        toggle_proxy(False)
        try: r = requests.get(url, headers=headers, timeout=10)
        except: return injuries

    if r and r.status_code == 200:
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
    return injuries

# 🔥 全新黑科技：讀取官方 CDN 靜態賽程表 (保證 0 阻擋)
def get_cdn_schedule():
    print("📥 正在從 NBA 官方 CDN 獲取完整賽季賽程表 (突破 Cloudflare 防線)...")
    url = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json'
    }
    toggle_proxy(False) # CDN 絕對不要掛 Proxy，直連最快！
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"❌ 官方 CDN 載入失敗: {e}")
        return None

def get_games_from_cdn(schedule_data, target_date_str):
    # CDN 日期格式可能是 "MM/DD/YYYY 00:00:00"
    dt_obj = datetime.strptime(target_date_str, '%Y-%m-%d')
    search_date = dt_obj.strftime('%m/%d/%Y')
    
    for date_node in schedule_data.get('leagueSchedule', {}).get('gameDates', []):
        node_date_str = str(date_node.get('gameDate', ''))
        # 兼容不同格式的日期字串
        if node_date_str.startswith(search_date) or target_date_str in node_date_str:
            return date_node.get('games', [])
    return []

def get_b2b_teams_cdn(schedule_data, us_date_str):
    yday_str = (datetime.strptime(us_date_str, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    games = get_games_from_cdn(schedule_data, yday_str)
    b2b_teams = []
    for game in games:
        # 只要有比賽就算 B2B，不論打完沒
        home_abbr = game.get('homeTeam', {}).get('teamTricode')
        away_abbr = game.get('awayTeam', {}).get('teamTricode')
        if home_abbr: b2b_teams.append(home_abbr)
        if away_abbr: b2b_teams.append(away_abbr)
    return b2b_teams

def fetch_and_save_upcoming_games():
    print("🚀 啟動無阻擋 CDN 賽程掃描器...")
    schedule_data = get_cdn_schedule()
    if not schedule_data:
        return
        
    tw_tz = timezone('Asia/Taipei')
    us_tz = timezone('US/Eastern')
    start_date = datetime.now(tw_tz) - timedelta(days=1)
    
    espn_injuries = scrape_espn_injuries()
    upcoming_games = []
    
    for i in range(7):
        target_date = start_date + timedelta(days=i)
        us_date_str = target_date.astimezone(us_tz).strftime('%Y-%m-%d')
        tw_date_str = (datetime.strptime(us_date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y%m%d')
        
        print(f"📡 正在過濾日期 {us_date_str}...")
        
        # 直接從記憶體內的 JSON 撈出當天比賽
        games = get_games_from_cdn(schedule_data, us_date_str)
        
        if not games: 
            print(f"   ⏩ {us_date_str} 沒有比賽資料")
            continue
            
        # 過濾掉已經打完的比賽 (gameStatus == 3 代表 Final)
        unplayed_games = [g for g in games if g.get('gameStatus') != 3]
        
        if not unplayed_games: 
            print(f"   ⏩ {us_date_str} 的比賽都打完了")
            continue
            
        print(f"   ✅ 找到 {len(unplayed_games)} 場未開打賽事，開始整理陣容與賠率...")
        
        yday_teams = get_b2b_teams_cdn(schedule_data, us_date_str)
        todays_odds = scrape_playsport_odds(tw_date_str)
        
        for game in unplayed_games:
            game_id = str(game.get('gameId')).zfill(10)
            home_abbr = game.get('homeTeam', {}).get('teamTricode')
            away_abbr = game.get('awayTeam', {}).get('teamTricode')
            status_text = game.get('gameStatusText', 'TBD')
            
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
                "game_date": us_date_str, "game_id": game_id,
                "home_team": home_abbr, "away_team": away_abbr,
                "status": status_text, 
                "vegas_spread": game_odds.get("spread", 0.0), "vegas_total": game_odds.get("total", 0.0),
                "home_is_b2b": home_abbr in yday_teams, "away_is_b2b": away_abbr in yday_teams,
                "home_injuries_ids": match_injuries(home_abbr, home_roster), 
                "away_injuries_ids": match_injuries(away_abbr, away_roster)
            })
            
        # 只要找到第一天的未開打賽事，就不用往後幾天找了
        if len(upcoming_games) > 0: break

    if upcoming_games:
        os.makedirs("data", exist_ok=True)
        # 去除重複的賽事 ID
        df = pd.DataFrame(upcoming_games).drop_duplicates(subset=['game_id'])
        df.to_csv("data/upcoming_games.csv", index=False, encoding="utf-8-sig")
        print(f"\n🎉 大功告成！成功儲存 {len(df)} 場賽程至 data/upcoming_games.csv！")
    else: 
        print("\n🤷‍♂️ 掃描完畢，未來 7 天內沒有找到任何未開打的賽事。")

if __name__ == "__main__":
    fetch_and_save_upcoming_games()