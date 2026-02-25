# SnapClub.py (Version 2.3 - Génère player_details.sqlite, Contrats.sqlite ET Agents.sqlite)
# Mise à jour : 
# 1. Ajout colonnes 'retraite' et 'manager'
# 2. Malus de -1 sur l'OVR calculé des postes secondaires
import os, json, time, logging, sys, threading, sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from datetime import datetime
from tqdm import tqdm
from r2_uploader import upload_file_to_r2

# ===================================================================
# CONFIGURATION
# ===================================================================
BATCH_SIZE = 1000
MAX_WORKERS = 10
PAUSE_BETWEEN_BATCHES_SECONDS = 310
ERROR_PAUSE_SECONDS = 300

# Noms des fichiers de sortie
PLAYERS_DB_PATH = "player_details.sqlite"
CONTRACTS_DB_PATH = "Contrats.sqlite"
AGENTS_DB_PATH = "Agents.sqlite"

# ... (le reste de la configuration est inchangé)
pause_event = threading.Event(); pause_lock = threading.Lock()
STATS_ORDER = ['passing', 'shooting', 'defense', 'dribbling', 'pace', 'physical']
_WEIGHTINGS_LIST = [{'positions': ['CB'], 'weights': [0.05, 0, 0.64, 0.09, 0.02, 0.2]},{'positions': ['LWB', 'RWB', 'LB', 'RB'], 'weights': [0.19, 0, 0.44, 0.17, 0.1, 0.1]},{'positions': ['CDM'], 'weights': [0.28, 0, 0.4, 0.17, 0, 0.15]},{'positions': ['CM', 'LM', 'RM'], 'weights': [0.43, 0.12, 0.1, 0.29, 0, 0.06]},{'positions': ['CAM'], 'weights': [0.34, 0.21, 0, 0.38, 0.07, 0]},{'positions': ['CF', 'LW', 'RW'], 'weights': [0.24, 0.23, 0, 0.4, 0.13, 0]},{'positions': ['ST'], 'weights': [0.1, 0.46, 0, 0.29, 0.1, 0.05]}]
WEIGHTINGS = {}
for item in _WEIGHTINGS_LIST:
    weight_dict = dict(zip(STATS_ORDER, item['weights']));
    for pos in item['positions']: WEIGHTINGS[pos] = weight_dict

# ===================================================================
# FONCTIONS DE COLLECTE DE DONNÉES
# ===================================================================
def setup_logging(log_file_name): logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(), logging.FileHandler(log_file_name, mode='w')])
def create_requests_session():
    session = requests.Session(); retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504]); adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=retries); session.mount('https://', adapter)
    return session

def get_agents_from_api():
    """Récupère la liste des agents depuis l'API globale en s'arrêtant quand nbPlayers est 0."""
    url = "https://z519wdyajg.execute-api.us-east-1.amazonaws.com/prod/leaderboards/users/global"
    params = {
        'sort': 'nbPlayers',
        'sortOrder': 'DESC',
        'limit': 20000
    }
    try: 
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        agents = []
        
        # Parcourir les agents et s'arrêter dès que nbPlayers est 0
        for agent in data.get('users', []):
            if agent.get('nbPlayers', 0) <= 0:
                break
            agents.append(agent)
            
        logging.info(f"Récupéré {len(agents)} agents avec des joueurs.")
        return agents
    except Exception as e: 
        logging.error(f"Erreur lors de la récupération des agents: {e}")
        return []

def calculate_real_note(stats, position):
    if position in ['GK', None] or position not in WEIGHTINGS: return int(stats.get('overall', 0))
    note = sum(stats.get(stat_name, 0) * weight for stat_name, weight in WEIGHTINGS[position].items()); return int(round(note))

def safe_request(url, session, timeout=30):
    for attempt in range(3):
        try: response = session.get(url, timeout=timeout); response.raise_for_status(); return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in (403, 429):
                with pause_lock:
                    if not pause_event.is_set(): logging.warning(f"{e.response.status_code} détecté..."); pause_event.set(); time.sleep(ERROR_PAUSE_SECONDS); pause_event.clear(); logging.info("Fin de la pause.")
                continue
            logging.error(f"Erreur HTTP: {e}"); time.sleep(1)
        except Exception as e: logging.error(f"Erreur de requête: {e}"); time.sleep(1)
    return None

def get_agent_snapshot(args):
    agent_wallet_address, session, index, total_in_batch = args; logging.info(f"  → [SNAP] Agent {agent_wallet_address} ({index}/{total_in_batch})")
    if pause_event.is_set(): pause_event.wait()
    try:
        url = f"https://z519wdyajg.execute-api.us-east-1.amazonaws.com/prod/players?limit=1500&ownerWalletAddress={agent_wallet_address}"; data = safe_request(url, session)
        if data is None: return {};
        if len(data) < 1500: return {str(item['id']): item for item in data}
        data_before = safe_request(f"{url}&beforePlayerId=80000", session); data_after = safe_request(f"{url}&afterPlayerId=80000", session)
        if data_before is None or data_after is None: return {}
        return {str(item['id']): item for item in data_before + data_after}
    except Exception as e: logging.error(f"  → Erreur SNAP pour agent {agent_wallet_address}: {e}"); return {}

def process_agent_batch(agent_wallets, process_func, session, **kwargs):
    all_data = {}; batches = [agent_wallets[i:i + BATCH_SIZE] for i in range(0, len(agent_wallets), BATCH_SIZE)]
    for i, batch in enumerate(batches):
        logging.info(f"\nLot {i+1}/{len(batches)} - {kwargs.get('description', '')}"); batch_data = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            args_list = [(wallet, session, j+1, len(batch)) for j, wallet in enumerate(batch)]; futures = {executor.submit(process_func, arg): arg[0] for arg in args_list}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result: batch_data.update(result)
                except Exception as e: logging.error(f"Erreur dans le thread pour {futures[future]}: {e}")
        all_data.update(batch_data)
        if kwargs.get('progress_bar'): kwargs['progress_bar'].update(len(batch))
        if i < len(batches) - 1: logging.info(f"→ Pause..."); time.sleep(PAUSE_BETWEEN_BATCHES_SECONDS)
    return all_data

# ===================================================================
# FONCTIONS DE MISE À JOUR BDD
# ===================================================================
def setup_databases():
    """Initialise les trois bases de données avec leurs schémas respectifs."""
    # Base de données des joueurs - SCHÉMA AVEC RETRAITE ET MANAGER
    with sqlite3.connect(PLAYERS_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY, 
            first_name TEXT, 
            last_name TEXT, 
            age INTEGER, 
            nationalities TEXT, 
            preferred_foot TEXT, 
            overall INTEGER, 
            defense INTEGER, 
            shooting INTEGER, 
            passing INTEGER, 
            dribbling INTEGER, 
            pace INTEGER, 
            physical INTEGER, 
            goalkeeping INTEGER, 
            real_notes TEXT, 
            retraite INTEGER, 
            manager TEXT,
            last_updated_at TEXT
        )""")
    
    # Base de données des contrats (sans la table clubs)
    with sqlite3.connect(CONTRACTS_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS contracts (player_id INTEGER PRIMARY KEY, player_first_name TEXT, player_last_name TEXT, player_age INTEGER, player_overall INTEGER, owner_wallet TEXT, owner_name TEXT, owner_twitter TEXT, status TEXT, kind TEXT, revenue_share INTEGER, total_revenue_share_locked INTEGER, club_id INTEGER, club_name TEXT, club_division INTEGER, nb_seasons INTEGER, created_date TEXT, clauses TEXT, last_updated_at TEXT)""")
    
    # Base de données des agents
    with sqlite3.connect(AGENTS_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS agents (wallet_address TEXT PRIMARY KEY, name TEXT, nb_players INTEGER, nb_clubs INTEGER, nb_trophies INTEGER, nb_mfl_points INTEGER, nb_mfl_points_last_season INTEGER, country TEXT, city TEXT, twitter TEXT, last_updated_at TEXT)""")

def update_agents_db(agents_data):
    """Met à jour la base de données Agents.sqlite."""
    now = datetime.utcnow().isoformat()
    agents_to_upsert = []
    
    for agent in agents_data:
        agents_to_upsert.append((
            agent.get('walletAddress'),
            agent.get('name'),
            agent.get('nbPlayers', 0),
            agent.get('nbClubs', 0),
            agent.get('nbTrophies', 0),
            agent.get('nbMflPoints', 0),
            agent.get('nbMflPointsLastSeason', 0),
            agent.get('country'),
            agent.get('city'),
            agent.get('twitter'),
            now
        ))
    
    with sqlite3.connect(AGENTS_DB_PATH) as conn:
        try:
            cursor = conn.cursor()
            sql = """INSERT INTO agents (wallet_address, name, nb_players, nb_clubs, nb_trophies, nb_mfl_points, nb_mfl_points_last_season, country, city, twitter, last_updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (wallet_address) DO UPDATE SET name=excluded.name, nb_players=excluded.nb_players, nb_clubs=excluded.nb_clubs, nb_trophies=excluded.nb_trophies, nb_mfl_points=excluded.nb_mfl_points, nb_mfl_points_last_season=excluded.nb_mfl_points_last_season, country=excluded.country, city=excluded.city, twitter=excluded.twitter, last_updated_at=excluded.last_updated_at;"""
            cursor.executemany(sql, agents_to_upsert)
            conn.commit()
            logging.info(f"✅ {len(agents_to_upsert)} agents mis à jour dans {AGENTS_DB_PATH}.")
        except Exception as e:
            logging.error(f"❌ Erreur BDD (agents): {e}")

def update_players_db(snapshot_data):
    """Met à jour la base de données player_details.sqlite."""
    now = datetime.utcnow().isoformat()
    players_to_upsert = []
    for player_id_str, p in snapshot_data.items():
        meta = p.get('metadata', {})
        owned_by = p.get('ownedBy', {})
        
        # Données supplémentaires
        manager_name = owned_by.get('name')
        retirement_years = meta.get('retirementYears')

        current_stats = {s: meta.get(s, 0) for s in STATS_ORDER + ['overall', 'goalkeeping']}
        positions = meta.get('positions', [])
        
        # LOGIQUE MISE A JOUR : Malus de -1 pour les postes secondaires
        real_notes = {}
        for i, pos in enumerate(positions):
            base_note = calculate_real_note(current_stats, pos)
            # Si index > 0 (c'est-à-dire pas le premier de la liste), on retire 1
            if i > 0:
                real_notes[pos] = base_note - 1
            else:
                real_notes[pos] = base_note
        
        players_to_upsert.append((
            int(player_id_str), 
            meta.get('firstName'), 
            meta.get('lastName'), 
            meta.get('age'), 
            json.dumps(meta.get('nationalities', [])), 
            meta.get('preferredFoot'), 
            current_stats['overall'], 
            current_stats['defense'], 
            current_stats['shooting'], 
            current_stats['passing'], 
            current_stats['dribbling'], 
            current_stats['pace'], 
            current_stats['physical'], 
            current_stats['goalkeeping'], 
            json.dumps(real_notes), 
            retirement_years,
            manager_name,
            now
        ))
    
    with sqlite3.connect(PLAYERS_DB_PATH) as conn:
        try:
            cursor = conn.cursor()
            sql = """INSERT INTO players (
                id, first_name, last_name, age, nationalities, preferred_foot, overall, 
                defense, shooting, passing, dribbling, pace, physical, goalkeeping, real_notes, 
                retraite, manager, last_updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
            ON CONFLICT (id) DO UPDATE SET 
                first_name=excluded.first_name, 
                last_name=excluded.last_name, 
                age=excluded.age, 
                nationalities=excluded.nationalities, 
                preferred_foot=excluded.preferred_foot, 
                overall=excluded.overall, 
                defense=excluded.defense, 
                shooting=excluded.shooting, 
                passing=excluded.passing, 
                dribbling=excluded.dribbling, 
                pace=excluded.pace, 
                physical=excluded.physical, 
                goalkeeping=excluded.goalkeeping, 
                real_notes=excluded.real_notes, 
                retraite=excluded.retraite, 
                manager=excluded.manager, 
                last_updated_at=excluded.last_updated_at;"""
            
            cursor.executemany(sql, players_to_upsert)
            conn.commit()
            logging.info(f"✅ {len(players_to_upsert)} joueurs mis à jour dans {PLAYERS_DB_PATH}.")
        except Exception as e:
            logging.error(f"❌ Erreur BDD (players): {e}")

def update_contracts_db(snapshot_data):
    """Met à jour la base de données Contrats.sqlite avec les informations du club."""
    now = datetime.utcnow().isoformat()
    contracts_to_upsert = []

    for player_id_str, p in snapshot_data.items():
        if 'activeContract' in p and p['activeContract']:
            meta = p.get('metadata', {})
            ownedBy = p.get('ownedBy', {})
            contract = p.get('activeContract', {})
            club = contract.get('club', {})
            
            contracts_to_upsert.append((
                int(player_id_str), meta.get('firstName'), meta.get('lastName'), meta.get('age'), meta.get('overall'),
                ownedBy.get('walletAddress'), ownedBy.get('name'), ownedBy.get('twitter'),
                contract.get('status'), contract.get('kind'), contract.get('revenueShare'), contract.get('totalRevenueShareLocked'),
                club.get('id'), club.get('name'), club.get('division'), contract.get('nbSeasons'), contract.get('createdDateTime'), 
                json.dumps(contract.get('clauses', [])), now
            ))

    with sqlite3.connect(CONTRACTS_DB_PATH) as conn:
        try:
            cursor = conn.cursor()
            # Mettre à jour les contrats
            sql_contracts = """INSERT INTO contracts (player_id, player_first_name, player_last_name, player_age, player_overall, owner_wallet, owner_name, owner_twitter, status, kind, revenue_share, total_revenue_share_locked, club_id, club_name, club_division, nb_seasons, created_date, clauses, last_updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (player_id) DO UPDATE SET player_first_name=excluded.player_first_name, player_last_name=excluded.player_last_name, player_age=excluded.player_age, player_overall=excluded.player_overall, owner_wallet=excluded.owner_wallet, owner_name=excluded.owner_name, owner_twitter=excluded.owner_twitter, status=excluded.status, kind=excluded.kind, revenue_share=excluded.revenue_share, total_revenue_share_locked=excluded.total_revenue_share_locked, club_id=excluded.club_id, club_name=excluded.club_name, club_division=excluded.club_division, nb_seasons=excluded.nb_seasons, created_date=excluded.created_date, clauses=excluded.clauses, last_updated_at=excluded.last_updated_at;"""
            cursor.executemany(sql_contracts, contracts_to_upsert)
            conn.commit()
            logging.info(f"✅ {len(contracts_to_upsert)} contrats mis à jour dans {CONTRACTS_DB_PATH}.")
        except Exception as e:
            logging.error(f"❌ Erreur BDD (contracts): {e}")

# ===================================================================
# SCRIPT PRINCIPAL
# ===================================================================
def main():
    start_time = time.time()
    setup_logging("SnapClub.log")
    logging.info(f"{'='*80}\nDÉMARRAGE DU SCRIPT SnapClub v2.3\n{'='*80}")
    
    setup_databases()
    session = create_requests_session()
    
    # Récupérer et mettre à jour les agents
    agents = get_agents_from_api()
    if not agents: 
        logging.critical("Aucun agent trouvé. Arrêt."); 
        return
    
    # Mettre à jour la base de données des agents
    update_agents_db(agents)
    
    # Récupérer les adresses des agents pour les snapshots
    agent_wallets = [agent['walletAddress'] for agent in agents]
    snapshot_bar = tqdm(total=len(agent_wallets), desc="[Snapshots] Agents", unit="agent")
    snapshot_data = process_agent_batch(agent_wallets, get_agent_snapshot, session, batch_size=BATCH_SIZE, description="SNAPSHOTS", progress_bar=snapshot_bar)
    snapshot_bar.close()

    if snapshot_data:
        logging.info("Mise à jour de la base de données des joueurs...")
        update_players_db(snapshot_data)
        logging.info("Mise à jour de la base de données des contrats...")
        update_contracts_db(snapshot_data)
    else:
        logging.warning("Aucune donnée de snapshot n'a été récupérée.")

    # TÉLÉVERSEMENT AUTOMATIQUE DES TROIS FICHIERS VERS R2
    logging.info("Début de l'upload des fichiers générés vers R2...")
    upload_file_to_r2(PLAYERS_DB_PATH, f"Players/{PLAYERS_DB_PATH}")
    upload_file_to_r2(CONTRACTS_DB_PATH, f"Clubs/Contrats.sqlite")
    upload_file_to_r2(AGENTS_DB_PATH, f"Agents/{AGENTS_DB_PATH}")

    logging.info(f"\n{'='*80}\nSCRIPT SnapClub v2.3 TERMINÉ\nFichiers générés: {PLAYERS_DB_PATH}, {CONTRACTS_DB_PATH}, {AGENTS_DB_PATH}\nDurée: {(time.time() - start_time) / 60:.2f} minutes\n{'='*80}")

if __name__ == "__main__":
    main()
