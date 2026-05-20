# SnapClub.py (Version 4.2 - Correction Datetime pour MariaDB BIGINT)
import os, json, time, logging, sys, sqlite3, requests
import mysql.connector
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from datetime import datetime
from tqdm import tqdm
from r2_uploader import upload_file_to_r2

# ===================================================================
# CONFIGURATION
# ===================================================================
PLAYERS_DB_PATH = "player_details.sqlite"
CONTRACTS_DB_PATH = "Contrats.sqlite"
AGENTS_DB_PATH = "Agents.sqlite"

LIMIT_PER_PAGE = 1500
SAFE_SLEEP = 0 

# CONFIGURATION MARIADB
DB_CONFIG = {
    'host': os.getenv("DB_HOST"),      
    'user': os.getenv("DB_USER", "ubuntu"),
    'password': os.getenv("DB_PASSWORD"), 
    'database': os.getenv("DB_NAME", "mfl_stats")
}

STATS_ORDER =['passing', 'shooting', 'defense', 'dribbling', 'pace', 'physical']
_WEIGHTINGS_LIST = [
    {'positions': ['CB'], 'weights':[0.05, 0, 0.64, 0.09, 0.02, 0.2]},
    {'positions': ['LWB', 'RWB', 'LB', 'RB'], 'weights':[0.19, 0, 0.44, 0.17, 0.1, 0.1]},
    {'positions': ['CDM'], 'weights':[0.28, 0, 0.4, 0.17, 0, 0.15]},
    {'positions': ['CM', 'LM', 'RM'], 'weights':[0.43, 0.12, 0.1, 0.29, 0, 0.06]},
    {'positions': ['CAM'], 'weights':[0.34, 0.21, 0, 0.38, 0.07, 0]},
    {'positions': ['CF', 'LW', 'RW'], 'weights':[0.24, 0.23, 0, 0.4, 0.13, 0]},
    {'positions':['ST'], 'weights':[0.1, 0.46, 0, 0.29, 0.1, 0.05]}
]
WEIGHTINGS = {}
for item in _WEIGHTINGS_LIST:
    weight_dict = dict(zip(STATS_ORDER, item['weights']))
    for pos in item['positions']: WEIGHTINGS[pos] = weight_dict

# ===================================================================
# UTILS & SESSION
# ===================================================================
def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.StreamHandler(), logging.FileHandler("SnapClub.log", mode='w')])

def create_requests_session():
    session = requests.Session()
    session.headers.update({'User-Agent': 'SnapClub-Bulk-v4.1', 'Accept': 'application/json'})
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[403, 429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def safe_request(url, session):
    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Erreur sur {url}: {e}")
        return None

def calculate_real_note(stats, position):
    if position in ['GK', None] or position not in WEIGHTINGS: return int(stats.get('overall', 0))
    note = sum(stats.get(stat_name, 0) * weight for stat_name, weight in WEIGHTINGS[position].items())
    return int(round(note))

# ===================================================================
# LOGIQUE MARIADB
# ===================================================================

def sync_to_mariadb_history(players_list):
    if not DB_CONFIG['host'] or not DB_CONFIG['password']:
        logging.warning("⚠️ Configuration MariaDB absente.")
        return

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        # now_ts est déjà un timestamp en ms, parfait pour la colonne BIGINT
        now_ts = int(time.time() * 1000)

        logging.info(f"Début de la synchronisation de {len(players_list)} joueurs...")

        for p in players_list:
            p_id = int(p['id'])
            meta = p.get('metadata', {})
            owned = p.get('ownedBy', {})
            
            new_stats = {
                'overall': meta.get('overall', 0),
                'age': meta.get('age', 0),
                'pace': meta.get('pace', 0),
                'shooting': meta.get('shooting', 0),
                'passing': meta.get('passing', 0),
                'dribbling': meta.get('dribbling', 0),
                'defense': meta.get('defense', 0),
                'physical': meta.get('physical', 0)
            }

            cursor.execute("SELECT * FROM players_snapshot WHERE id = %s", (p_id,))
            old_data = cursor.fetchone()

            if old_data:
                diff = {stat: val for stat, val in new_stats.items() if val != old_data.get(stat)}
                if diff:
                    reason = "NEW_AGE" if 'age' in diff else "TRAINING"
                    cursor.execute(
                        "INSERT INTO players_history (player_id, date, reasonType, values_changed) VALUES (%s, %s, %s, %s)",
                        (p_id, now_ts, reason, json.dumps(diff))
                    )

            stats_temp = {s: meta.get(s, 0) for s in STATS_ORDER + ['overall', 'goalkeeping']}
            real_notes = {pos: (calculate_real_note(stats_temp, pos) - (1 if i > 0 else 0)) for i, pos in enumerate(meta.get('positions',[]))}

            sql_snap = """
                INSERT INTO players_snapshot 
                (id, first_name, last_name, age, nationalities, preferred_foot, overall, defense, shooting, passing, dribbling, pace, physical, goalkeeping, real_notes, retraite, manager, last_updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                first_name=VALUES(first_name), last_name=VALUES(last_name), age=VALUES(age),
                nationalities=VALUES(nationalities), preferred_foot=VALUES(preferred_foot),
                overall=VALUES(overall), defense=VALUES(defense), shooting=VALUES(shooting),
                passing=VALUES(passing), dribbling=VALUES(dribbling), pace=VALUES(pace),
                physical=VALUES(physical), goalkeeping=VALUES(goalkeeping), real_notes=VALUES(real_notes),
                retraite=VALUES(retraite), manager=VALUES(manager), last_updated_at=VALUES(last_updated_at)
            """
            # Utilisation de now_ts (entier) au lieu de now_str (chaine) pour correspondre au BIGINT
            cursor.execute(sql_snap, (
                p_id, meta.get('firstName'), meta.get('lastName'), new_stats['age'],
                json.dumps(meta.get('nationalities',[])), meta.get('preferredFoot'),
                new_stats['overall'], meta.get('defense', 0), new_stats['shooting'],
                new_stats['passing'], new_stats['dribbling'], new_stats['pace'],
                new_stats['physical'], meta.get('goalkeeping', 0), json.dumps(real_notes),
                meta.get('retirementYears', 0), owned.get('name'), now_ts
            ))

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("✅ MariaDB synchronisée avec succès.")
    except Exception as e:
        logging.error(f"❌ Erreur MariaDB : {e}")

# ===================================================================
# COLLECTE & SQLite (Reste du code inchangé)
# ... [Le reste du code est identique à votre version] ...
# ===================================================================
# ===================================================================
# COLLECTE & SQLITE D'ORIGINE
# ===================================================================

def get_agents_leaderboard(session):
    url = "https://z519wdyajg.execute-api.us-east-1.amazonaws.com/prod/leaderboards/users/global?limit=20000&sort=nbPlayers&sortOrder=DESC"
    data = safe_request(url, session)
    return [a for a in data.get('users', []) if a.get('nbPlayers', 0) > 0] if data else []

def get_all_players_bulk(session):
    all_players = []
    last_id = None
    pbar = tqdm(desc="[Collecte] Pages", unit=" page")
    while True:
        url = f"https://z519wdyajg.execute-api.us-east-1.amazonaws.com/prod/players?limit={LIMIT_PER_PAGE}"
        if last_id: url += f"&beforePlayerId={last_id}"
        data = safe_request(url, session)
        if not data: break
        all_players.extend(data)
        last_id = data[-1]['id']
        pbar.update(1)
        time.sleep(SAFE_SLEEP)
    pbar.close()
    return all_players

def setup_databases():
    with sqlite3.connect(PLAYERS_DB_PATH) as conn:
        conn.cursor().execute("""CREATE TABLE IF NOT EXISTS players (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, age INTEGER, nationalities TEXT, preferred_foot TEXT, overall INTEGER, defense INTEGER, shooting INTEGER, passing INTEGER, dribbling INTEGER, pace INTEGER, physical INTEGER, goalkeeping INTEGER, real_notes TEXT, retraite INTEGER, manager TEXT, last_updated_at TEXT)""")
    with sqlite3.connect(CONTRACTS_DB_PATH) as conn:
        conn.cursor().execute("""CREATE TABLE IF NOT EXISTS contracts (player_id INTEGER PRIMARY KEY, player_first_name TEXT, player_last_name TEXT, player_age INTEGER, player_overall INTEGER, owner_wallet TEXT, owner_name TEXT, owner_twitter TEXT, status TEXT, kind TEXT, revenue_share INTEGER, total_revenue_share_locked INTEGER, club_id INTEGER, club_name TEXT, club_division INTEGER, nb_seasons INTEGER, created_date TEXT, clauses TEXT, last_updated_at TEXT)""")
    with sqlite3.connect(AGENTS_DB_PATH) as conn:
        conn.cursor().execute("""CREATE TABLE IF NOT EXISTS agents (wallet_address TEXT PRIMARY KEY, name TEXT, nb_players INTEGER, nb_clubs INTEGER, nb_trophies INTEGER, nb_mfl_points INTEGER, nb_mfl_points_last_season INTEGER, country TEXT, city TEXT, twitter TEXT, last_updated_at TEXT)""")

def update_all_databases(players_list, agents_list):
    now = datetime.utcnow().isoformat()
    # 1. Agents
    agents_data = [(a.get('walletAddress'), a.get('name'), a.get('nbPlayers', 0), a.get('nbClubs', 0), a.get('nbTrophies', 0), a.get('nbMflPoints', 0), a.get('nbMflPointsLastSeason', 0), a.get('country'), a.get('city'), a.get('twitter'), now) for a in agents_list]
    with sqlite3.connect(AGENTS_DB_PATH) as conn:
        conn.cursor().executemany("INSERT INTO agents VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(wallet_address) DO UPDATE SET name=excluded.name, nb_players=excluded.nb_players, last_updated_at=excluded.last_updated_at", agents_data)
    # 2. Players & Contracts
    players_data = []
    contracts_data = []
    for p in players_list:
        meta = p.get('metadata', {}); owned = p.get('ownedBy', {})
        stats = {s: meta.get(s, 0) for s in STATS_ORDER + ['overall', 'goalkeeping']}
        real_notes = {pos: (calculate_real_note(stats, pos) - (1 if i > 0 else 0)) for i, pos in enumerate(meta.get('positions', []))}
        players_data.append((int(p['id']), meta.get('firstName'), meta.get('lastName'), meta.get('age'), json.dumps(meta.get('nationalities', [])), meta.get('preferredFoot'), stats['overall'], stats['defense'], stats['shooting'], stats['passing'], stats['dribbling'], stats['pace'], stats['physical'], stats['goalkeeping'], json.dumps(real_notes), meta.get('retirementYears'), owned.get('name'), now))
        if p.get('activeContract'):
            cont = p['activeContract']; club = cont.get('club', {})
            contracts_data.append((int(p['id']), meta.get('firstName'), meta.get('lastName'), meta.get('age'), meta.get('overall'), owned.get('walletAddress'), owned.get('name'), owned.get('twitter'), cont.get('status'), cont.get('kind'), cont.get('revenueShare'), cont.get('totalRevenueShareLocked'), club.get('id'), club.get('name'), club.get('division'), cont.get('nbSeasons'), cont.get('createdDateTime'), json.dumps(cont.get('clauses', [])), now))
    
    with sqlite3.connect(PLAYERS_DB_PATH) as conn:
        conn.cursor().executemany("INSERT INTO players VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET overall=excluded.overall, last_updated_at=excluded.last_updated_at", players_data)
    with sqlite3.connect(CONTRACTS_DB_PATH) as conn:
        conn.cursor().executemany("INSERT INTO contracts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(player_id) DO UPDATE SET last_updated_at=excluded.last_updated_at", contracts_data)

# ===================================================================
# MAIN
# ===================================================================
def main():
    start_time = time.time()
    setup_logging()
    setup_databases()
    session = create_requests_session()
    
    agents = get_agents_leaderboard(session)
    players = get_all_players_bulk(session)
    
    if players:
        # 1. SQLite Local (pour Netlify/R2)
        update_all_databases(players, agents)
        
        # 2. MariaDB Oracle (pour l'historique)
        sync_to_mariadb_history(players)
        
        # 3. Upload vers R2
        upload_file_to_r2(PLAYERS_DB_PATH, f"Players/{PLAYERS_DB_PATH}")
        upload_file_to_r2(CONTRACTS_DB_PATH, f"Clubs/Contrats.sqlite")
        upload_file_to_r2(AGENTS_DB_PATH, f"Agents/{AGENTS_DB_PATH}")
    
    logging.info(f"TERMINÉ en {(time.time() - start_time) / 60:.2f} minutes.")

if __name__ == "__main__":
    main()
