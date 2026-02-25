# r2_uploader.py
import os
import boto3
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_r2_config():
    if 'GITHUB_ACTIONS' in os.environ:
        logging.info("Détecté: Environnement GitHub Actions. Utilisation des secrets.")
        account_id = os.getenv("R2_ACCOUNT_ID")
        bucket_name = os.getenv("R2_BUCKET_NAME")
        access_key_id = os.getenv("R2_ACCESS_KEY_ID")
        secret_access_key = os.getenv("R2_SECRET_ACCESS_KEY")
    else:
        logging.info("Détecté: Environnement local. Chargement du fichier .env.")
        load_dotenv()
        account_id = os.getenv("R2_ACCOUNT_ID")
        bucket_name = os.getenv("R2_BUCKET_NAME")
        access_key_id = os.getenv("R2_ACCESS_KEY_ID")
        secret_access_key = os.getenv("R2_SECRET_ACCESS_KEY")

    if not all([account_id, bucket_name, access_key_id, secret_access_key]):
        logging.error("Configuration R2 incomplète. Vérifiez vos secrets GitHub ou votre fichier .env.")
        return None
    
    return {
        "endpoint_url": f"https://{account_id}.r2.cloudflarestorage.com",
        "bucket_name": bucket_name,
        "aws_access_key_id": access_key_id,
        "aws_secret_access_key": secret_access_key
    }

def upload_file_to_r2(local_file_path, r2_destination_path):
    config = get_r2_config()
    if not config:
        return

    if not os.path.exists(local_file_path):
        logging.warning(f"[Upload R2] Fichier local non trouvé, upload ignoré : '{local_file_path}'")
        return

    logging.info(f"[Upload R2] Tentative d'upload de '{local_file_path}' vers '{r2_destination_path}'...")
    
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=config["endpoint_url"],
            aws_access_key_id=config["aws_access_key_id"],
            aws_secret_access_key=config["aws_secret_access_key"],
            region_name='auto'
        )
        s3_client.upload_file(local_file_path, config["bucket_name"], r2_destination_path)
        logging.info(f"[Upload R2] ✅ Upload réussi pour '{local_file_path}'.")
    except Exception as e:
        logging.error(f"[Upload R2] ❌ Échec de l'upload pour '{local_file_path}': {e}")
