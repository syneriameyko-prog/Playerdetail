# SnapClub.py (Version 2.4 - Optimisée sans pauses de 5 minutes)
import os, json, time, logging, sys, threading, sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from datetime import datetime
from tqdm import tqdm
from r2_uploader import upload_file_to_r2

# ===================================================================
# CONFIGURATION OPTIMISÉE (FLUX CONTINU)
# ===================================================================
BATCH_SIZE = 30000              # On traite tout en un seul bloc
MAX_WORKERS = 3                 # On limite à 3 requêtes simultanées (conseil Discord)
PAUSE_BETWEEN_BATCHES_SECONDS = 0 # Plus de pause entre les lots
ERROR_PAUSE_SECONDS = 20        # Si 403, on attend 20s au lieu de 5 min

# Noms des fichiers de sortie
PLAYERS_DB_PATH = "player_details.sqlite"
CONTRACTS_DB_PATH = "Contrats.sqlite"
AGENTS_DB_PATH = "Agents.sqlite"

pause_event = threading.Event()
pause_lock = threading.Lock()

STATS_ORDER = ['passing', 'shooting', 'defense', 'dribbling', 'pace', 'physical']
_WEIGHTINGS_LIST = [
    {'positions': ['CB'], 'weights': [0.05, 0, 0.64, 0.09, 0.02, 0.2]},
    {'positions': ['LWB', 'RWB', 'LB', 'RB'], 'weights': [0.19, 0, 0.44, 0.17, 0.1, 0.1]},
    {'positions': ['CDM'], 'weights': [0.28, 0, 0.4, 0.17, 0, 0.15]},
    {'positions': ['CM', 'LM', 'RM'], 'weights': [0.43, 0.12, 0.1, 0.29, 0, 0.06]},
    {'positions': ['CAM'], 'weights': [0.34, 0.21, 0, 0.38, 0.07, 0]},
    {'positions': ['CF', 'LW', 'RW'], 'weights': [0.24, 0.23, 0, 0.4, 0.13, 0]},
    {'positions': ['ST'], 'weights': [0.1, 0.46, 0, 0.29, 0.1, 0.05]}
]
WEIGHTINGS = {}
for item in _WEIGHTINGS_LIST:
    weight_dict = dict(zip(STATS_ORDER, item['weights']))
    for pos in item['positions']: WEIGHTINGS[pos] = weight_dict

# ===================================================================
# FONCTIONS DE COLLECTE DE DONNÉES
# ===================================================================
def setup_logging(log_file_name): 
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', 
                        handlers=[logging.StreamHandler(), logging.FileHandler(log_file_name, mode='w')])

def create_requests_session():
    session = requests.Session()
    # Identification polie auprès de l'API MFL
    session.headers.update({
        'User-Agent': 'SnapClub-Bot-v2.4 (Community Tool)',
        'Accept': 'application/json'
    })
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=retries)
    session.mount('https://', adapter)
    return session

def get_agents_from_api():
    url = "https://z519wdyajg.execute-api.us-east-1.amazonaws.com/prod/leaderboards/users/global"
    params = {'sort': 'nbPlayers', 'sortOrder': 'DESC', 'limit': 20000}
    try: 
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        agents = [a for a in data.get('users', []) if a.get('nbPlayers', 0) > 0]
        logging.info(f"Récupéré {len(agents)} agents avec des joueurs.")
        return agents
    except Exception as e: 
        logging.error(f"Erreur lors de la récupération des agents: {e}")
        return []

def calculate_real_note(stats, position):
    if position in ['GK', None] or position not in WEIGHTINGS: return int(stats.get('overall', 0))
    note = sum(stats.get(stat_name, 0) * weight for stat_name, weight in WEIGHTINGS[position].items())
    return int(round(note))

def safe_request(url, session, timeout=30):
    for attempt in range(3):
        try: 
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in (403, 429):
                logging.warning(f"Blocage {e.response.status_code}. Pause de sécurité ({ERROR_PAUSE_SECONDS}s)...")
                time.sleep(ERROR_PAUSE_SECONDS)
                continue
            logging.error(f"Erreur HTTP: {e}"); time.sleep(2)
        except Exception as e: 
            logging.error(f"Erreur de requête: {e}"); time.sleep(2)
    return None

def get_agent_snapshot(args):
    agent_wallet_address, session, index, total_in_batch = args
    
    # Micro-délai pour maintenir un débit constant et ne pas saturer l'API
    # 3 workers avec 0.7s de pause = environ 4.2 requêtes/seconde. (C'est la vitesse idéale)
    time.sleep(1.0)
    
    logging.info(f"  → [SNAP] Agent {agent_wallet_address} ({index}/{total_in_batch})")
    try:
        url = f"https://z519wdyajg.execute-api.us-east-1.amazonaws.com/prod/players?limit=1500&ownerWalletAddress={agent_wallet_address}"
        data = safe_request(url, session)
        if data is None: return {}
        if len(data) < 1500: return {str(item['id']): item for item in data}
        
        # Gestion des agents ayant + de 1500 joueurs
        data_before = safe_request(f"{url}&beforePlayerId=80000", session)
        data_after = safe_request(f"{url}&afterPlayerId=80000", session)
        if data_before is None or data_after is None: return {}
        return {str(item['id']): item for item in data_before + data_after}
    except Exception as e: 
        logging.error(f"  → Erreur SNAP pour agent {agent_wallet_address}: {e}")
        return {}

def process_agent_batch(agent_wallets, process_func, session, **kwargs):
    all_data = {}
    # On traite tout d'un coup sans découper en lots de 5 minutes
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        args_list = [(wallet, session, j+1, len(agent_wallets)) for j, wallet in enumerate(agent_wallets)]
        futures = {executor.submit(process_func, arg): arg[0] for arg in args_list}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result: all_data.update(result)
                if kwargs.get('progress_bar'): kwargs['progress_bar'].update(1)
            except Exception as e: 
                logging.error(f"Erreur dans le thread: {e}")
    return all_data

# ===================================================================
# FONCTIONS DE MISE À JOUR BDD (Inchangées)
# ===================================================================
def setup_databases():
    with sqlite3.connect(PLAYERS_DB_PATH) as conn:
        conn.cursor().execute("""CREATE TABLE IF NOT EXISTS players (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, age INTEGER, nationalities TEXT, preferred_foot TEXT, overall INTEGER, defense INTEGER, shooting INTEGER, passing INTEGER, dribbling INTEGER, pace INTEGER, physical INTEGER, goalkeeping INTEGER, real_notes TEXT, retraite INTEGER, manager TEXT, last_updated_at TEXT)""")
    with sqlite3.connect(CONTRACTS_DB_PATH) as conn:
        conn.cursor().execute("""CREATE TABLE IF NOT EXISTS contracts (player_id INTEGER PRIMARY KEY, player_first_name TEXT, player_last_name TEXT, player_age INTEGER, player_overall INTEGER, owner_wallet TEXT, owner_name TEXT, owner_twitter TEXT, status TEXT, kind TEXT, revenue_share INTEGER, total_revenue_share_locked INTEGER, club_id INTEGER, club_name TEXT, club_division INTEGER, nb_seasons INTEGER, created_date TEXT, clauses TEXT, last_updated_at TEXT)""")
    with sqlite3.connect(AGENTS_DB_PATH) as conn:
        conn.cursor().execute("""CREATE TABLE IF NOT EXISTS agents (wallet_address TEXT PRIMARY KEY, name TEXT, nb_players INTEGER, nb_clubs INTEGER, nb_trophies INTEGER, nb_mfl_points INTEGER, nb_mfl_points_last_season INTEGER, country TEXT, city TEXT, twitter TEXT, last_updated_at TEXT)""")

def update_agents_db(agents_data):
    now = datetime.utcnow().isoformat()
    agents_to_upsert = [(a.get('walletAddress'), a.get('name'), a.get('nbPlayers', 0), a.get('nbClubs', 0), a.get('nbTrophies', 0), a.get('nbMflPoints', 0), a.get('nbMflPointsLastSeason', 0), a.get('country'), a.get('city'), a.get('twitter'), now) for a in agents_data]
    with sqlite3.connect(AGENTS_DB_PATH) as conn:
        sql = """INSERT INTO agents (wallet_address, name, nb_players, nb_clubs, nb_trophies, nb_mfl_points, nb_mfl_points_last_season, country, city, twitter, last_updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (wallet_address) DO UPDATE SET name=excluded.name, nb_players=excluded.nb_players, nb_clubs=excluded.nb_clubs, nb_trophies=excluded.nb_trophies, nb_mfl_points=excluded.nb_mfl_points, nb_mfl_points_last_season=excluded.nb_mfl_points_last_season, country=excluded.country, city=excluded.city, twitter=excluded.twitter, last_updated_at=excluded.last_updated_at;"""
        conn.cursor().executemany(sql, agents_to_upsert)

def update_players_db(snapshot_data):
    now = datetime.utcnow().isoformat()
    players_to_upsert = []
    for p_id, p in snapshot_data.items():
        meta = p.get('metadata', {}); owned = p.get('ownedBy', {})
        stats = {s: meta.get(s, 0) for s in STATS_ORDER + ['overall', 'goalkeeping']}
        pos_list = meta.get('positions', [])
        real_notes = {pos: (calculate_real_note(stats, pos) - (1 if i > 0 else 0)) for i, pos in enumerate(pos_list)}
        players_to_upsert.append((int(p_id), meta.get('firstName'), meta.get('lastName'), meta.get('age'), json.dumps(meta.get('nationalities', [])), meta.get('preferredFoot'), stats['overall'], stats['defense'], stats['shooting'], stats['passing'], stats['dribbling'], stats['pace'], stats['physical'], stats['goalkeeping'], json.dumps(real_notes), meta.get('retirementYears'), owned.get('name'), now))
    with sqlite3.connect(PLAYERS_DB_PATH) as conn:
        sql = """INSERT INTO players (id, first_name, last_name, age, nationalities, preferred_foot, overall, defense, shooting, passing, dribbling, pace, physical, goalkeeping, real_notes, retraite, manager, last_updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (id) DO UPDATE SET first_name=excluded.first_name, last_name=excluded.last_name, age=excluded.age, nationalities=excluded.nationalities, preferred_foot=excluded.preferred_foot, overall=excluded.overall, defense=excluded.defense, shooting=excluded.shooting, passing=excluded.passing, dribbling=excluded.dribbling, pace=excluded.pace, physical=excluded.physical, goalkeeping=excluded.goalkeeping, real_notes=excluded.real_notes, retraite=excluded.retraite, manager=excluded.manager, last_updated_at=excluded.last_updated_at;"""
        conn.cursor().executemany(sql, players_to_upsert)

def update_contracts_db(snapshot_data):
    now = datetime.utcnow().isoformat()
    contracts_to_upsert = []
    for p_id, p in snapshot_data.items():
        if p.get('activeContract'):
            meta = p.get('metadata', {}); owned = p.get('ownedBy', {}); cont = p['activeContract']; club = cont.get('club', {})
            contracts_to_upsert.append((int(p_id), meta.get('firstName'), meta.get('lastName'), meta.get('age'), meta.get('overall'), owned.get('walletAddress'), owned.get('name'), owned.get('twitter'), cont.get('status'), cont.get('kind'), cont.get('revenueShare'), cont.get('totalRevenueShareLocked'), club.get('id'), club.get('name'), club.get('division'), cont.get('nbSeasons'), cont.get('createdDateTime'), json.dumps(cont.get('clauses', [])), now))
    with sqlite3.connect(CONTRACTS_DB_PATH) as conn:
        sql = """INSERT INTO contracts (player_id, player_first_name, player_last_name, player_age, player_overall, owner_wallet, owner_name, owner_twitter, status, kind, revenue_share, total_revenue_share_locked, club_id, club_name, club_division, nb_seasons, created_date, clauses, last_updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (player_id) DO UPDATE SET player_first_name=excluded.player_first_name, player_last_name=excluded.player_last_name, player_age=excluded.player_age, player_overall=excluded.player_overall, owner_wallet=excluded.owner_wallet, owner_name=excluded.owner_name, owner_twitter=excluded.owner_twitter, status=excluded.status, kind=excluded.kind, revenue_share=excluded.revenue_share, total_revenue_share_locked=excluded.total_revenue_share_locked, club_id=excluded.club_id, club_name=excluded.club_name, club_division=excluded.club_division, nb_seasons=excluded.nb_seasons, created_date=excluded.created_date, clauses=excluded.clauses, last_updated_at=excluded.last_updated_at;"""
        conn.cursor().executemany(sql, contracts_to_upsert)

# ===================================================================
# SCRIPT PRINCIPAL
# ===================================================================
def main():
    start_time = time.time()
    setup_logging("SnapClub.log")
    logging.info(f"{'='*80}\nDÉMARRAGE DU SCRIPT SnapClub v2.4 (Mode Fluide)\n{'='*80}")
    
    setup_databases()
    session = create_requests_session()
    
    agents = get_agents_from_api()
    if not agents: return
    
    update_agents_db(agents)
    
    agent_wallets = [agent['walletAddress'] for agent in agents]
    snapshot_bar = tqdm(total=len(agent_wallets), desc="[Snapshots] Agents", unit="agent")
    
    # Lancement du traitement fluide
    snapshot_data = process_agent_batch(agent_wallets, get_agent_snapshot, session, progress_bar=snapshot_bar)
    snapshot_bar.close()

    if snapshot_data:
        logging.info("Mise à jour des BDD Players et Contracts...")
        update_players_db(snapshot_data)
        update_contracts_db(snapshot_data)

    logging.info("Téléversement vers Cloudflare R2...")
    upload_file_to_r2(PLAYERS_DB_PATH, f"Players/{PLAYERS_DB_PATH}")
    upload_file_to_r2(CONTRACTS_DB_PATH, f"Clubs/Contrats.sqlite")
    upload_file_to_r2(AGENTS_DB_PATH, f"Agents/{AGENTS_DB_PATH}")

    logging.info(f"\n{'='*80}\nTERMINÉ en {(time.time() - start_time) / 60:.2f} minutes\n{'='*80}")

if __name__ == "__main__":
    main()
