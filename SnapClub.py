# SnapClub.py (Version 3.0 - Mode Vrac par Pagination)
import os, json, time, logging, sys, sqlite3, requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from datetime import datetime
from tqdm import tqdm
from r2_uploader import upload_file_to_r2

# ===================================================================
# CONFIGURATION
# ===================================================================
# Noms des fichiers de sortie
PLAYERS_DB_PATH = "player_details.sqlite"
CONTRACTS_DB_PATH = "Contrats.sqlite"
AGENTS_DB_PATH = "Agents.sqlite"

# Paramètres de l'API
LIMIT_PER_PAGE = 1500
MAX_WORKERS = 2
SAFE_SLEEP = 0  # Petite pause entre chaque page de 1500 joueurs

# Configuration des poids pour le Real OVR
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
# UTILS & SESSION
# ===================================================================
def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.StreamHandler(), logging.FileHandler("SnapClub.log", mode='w')])

def create_requests_session():
    session = requests.Session()
    session.headers.update({'User-Agent': 'SnapClub-Bulk-v3.0', 'Accept': 'application/json'})
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[403, 429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def safe_request(url, session):
    try:
        response = session.get(url, timeout=60) # 60s car les pages de 1500 sont lourdes
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
# COLLECTE DES DONNÉES
# ===================================================================

def get_agents_leaderboard(session):
    """Récupère le leaderboard des agents (nécessaire pour Agents.sqlite)."""
    logging.info("Récupération du leaderboard des agents...")
    url = "https://z519wdyajg.execute-api.us-east-1.amazonaws.com/prod/leaderboards/users/global?limit=20000&sort=nbPlayers&sortOrder=DESC"
    data = safe_request(url, session)
    if data:
        return [a for a in data.get('users', []) if a.get('nbPlayers', 0) > 0]
    return []

def get_all_players_bulk(session):
    """Récupère TOUS les joueurs de l'API par pages de 1500 (méthode Vrac)."""
    all_players = []
    last_id = None
    
    # On affiche une barre de progression (estimation ~250 pages pour 370k joueurs)
    pbar = tqdm(desc="[Collecte] Pages de joueurs", unit=" page")
    
    while True:
        url = f"https://z519wdyajg.execute-api.us-east-1.amazonaws.com/prod/players?limit={LIMIT_PER_PAGE}"
        if last_id:
            url += f"&beforePlayerId={last_id}"
        
        data = safe_request(url, session)
        if not data or len(data) == 0:
            break # Plus de joueurs à récupérer
            
        all_players.extend(data)
        last_id = data[-1]['id'] # On prend l'ID du dernier joueur pour la page suivante
        
        pbar.update(1)
        time.sleep(SAFE_SLEEP) # Respect de l'API
        
    pbar.close()
    logging.info(f"Total collecté : {len(all_players)} joueurs.")
    return all_players

# ===================================================================
# MISE À JOUR BASES DE DONNÉES
# ===================================================================

def setup_databases():
    with sqlite3.connect(PLAYERS_DB_PATH) as conn:
        conn.cursor().execute("""CREATE TABLE IF NOT EXISTS players (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, age INTEGER, nationalities TEXT, preferred_foot TEXT, overall INTEGER, defense INTEGER, shooting INTEGER, passing INTEGER, dribbling INTEGER, pace INTEGER, physical INTEGER, goalkeeping INTEGER, real_notes TEXT, retraite INTEGER, manager TEXT, last_updated_at TEXT)""")
    with sqlite3.connect(CONTRACTS_DB_PATH) as conn:
        conn.cursor().execute("""CREATE TABLE IF NOT EXISTS contracts (player_id INTEGER PRIMARY KEY, player_first_name TEXT, player_last_name TEXT, player_age INTEGER, player_overall INTEGER, owner_wallet TEXT, owner_name TEXT, owner_twitter TEXT, status TEXT, kind TEXT, revenue_share INTEGER, total_revenue_share_locked INTEGER, club_id INTEGER, club_name TEXT, club_division INTEGER, nb_seasons INTEGER, created_date TEXT, clauses TEXT, last_updated_at TEXT)""")
    with sqlite3.connect(AGENTS_DB_PATH) as conn:
        conn.cursor().execute("""CREATE TABLE IF NOT EXISTS agents (wallet_address TEXT PRIMARY KEY, name TEXT, nb_players INTEGER, nb_clubs INTEGER, nb_trophies INTEGER, nb_mfl_points INTEGER, nb_mfl_points_last_season INTEGER, country TEXT, city TEXT, twitter TEXT, last_updated_at TEXT)""")

def update_all_databases(players_list, agents_list):
    now = datetime.utcnow().isoformat()
    
    # 1. Mise à jour Agents
    agents_data = [(a.get('walletAddress'), a.get('name'), a.get('nbPlayers', 0), a.get('nbClubs', 0), a.get('nbTrophies', 0), a.get('nbMflPoints', 0), a.get('nbMflPointsLastSeason', 0), a.get('country'), a.get('city'), a.get('twitter'), now) for a in agents_list]
    with sqlite3.connect(AGENTS_DB_PATH) as conn:
        conn.cursor().executemany("INSERT INTO agents VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(wallet_address) DO UPDATE SET name=excluded.name, nb_players=excluded.nb_players, last_updated_at=excluded.last_updated_at", agents_data)

    # 2. Préparation Players et Contracts
    players_data = []
    contracts_data = []
    
    logging.info("Traitement des données et calcul des Real OVR...")
    for p in tqdm(players_list, desc="[Traitement] Joueurs"):
        meta = p.get('metadata', {})
        owned = p.get('ownedBy', {})
        stats = {s: meta.get(s, 0) for s in STATS_ORDER + ['overall', 'goalkeeping']}
        pos_list = meta.get('positions', [])
        
        # Calcul Real Notes avec malus de -1 pour postes secondaires
        real_notes = {pos: (calculate_real_note(stats, pos) - (1 if i > 0 else 0)) for i, pos in enumerate(pos_list)}
        
        players_data.append((int(p['id']), meta.get('firstName'), meta.get('lastName'), meta.get('age'), json.dumps(meta.get('nationalities', [])), meta.get('preferredFoot'), stats['overall'], stats['defense'], stats['shooting'], stats['passing'], stats['dribbling'], stats['pace'], stats['physical'], stats['goalkeeping'], json.dumps(real_notes), meta.get('retirementYears'), owned.get('name'), now))
        
        if p.get('activeContract'):
            cont = p['activeContract']; club = cont.get('club', {})
            contracts_data.append((int(p['id']), meta.get('firstName'), meta.get('lastName'), meta.get('age'), meta.get('overall'), owned.get('walletAddress'), owned.get('name'), owned.get('twitter'), cont.get('status'), cont.get('kind'), cont.get('revenueShare'), cont.get('totalRevenueShareLocked'), club.get('id'), club.get('name'), club.get('division'), cont.get('nbSeasons'), cont.get('createdDateTime'), json.dumps(cont.get('clauses', [])), now))

    # Injection massive en BDD
    with sqlite3.connect(PLAYERS_DB_PATH) as conn:
        conn.cursor().executemany("INSERT INTO players VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET overall=excluded.overall, manager=excluded.manager, last_updated_at=excluded.last_updated_at", players_data)
    
    with sqlite3.connect(CONTRACTS_DB_PATH) as conn:
        conn.cursor().executemany("INSERT INTO contracts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(player_id) DO UPDATE SET club_name=excluded.club_name, last_updated_at=excluded.last_updated_at", contracts_data)

# ===================================================================
# MAIN
# ===================================================================
def main():
    start_time = time.time()
    setup_logging()
    logging.info(f"{'='*60}\nSNAPCLUB v3.0 - DÉMARRAGE\n{'='*60}")
    
    setup_databases()
    session = create_requests_session()
    
    # 1. Leaderboard Agents
    agents = get_agents_leaderboard(session)
    
    # 2. Tous les joueurs en vrac (La partie la plus longue)
    players = get_all_players_bulk(session)
    
    # 3. Traitement et BDD
    if players:
        update_all_databases(players, agents)
        
        # 4. Upload vers R2
        logging.info("Téléversement vers Cloudflare R2...")
        upload_file_to_r2(PLAYERS_DB_PATH, f"Players/{PLAYERS_DB_PATH}")
        upload_file_to_r2(CONTRACTS_DB_PATH, f"Clubs/Contrats.sqlite")
        upload_file_to_r2(AGENTS_DB_PATH, f"Agents/{AGENTS_DB_PATH}")
    
    logging.info(f"TERMINÉ en {(time.time() - start_time) / 60:.2f} minutes.")

if __name__ == "__main__":
    main()
