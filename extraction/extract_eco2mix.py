import argparse
import logging 
import sys 
import time 
from datetime import datetime, timezone
from pathlib import Path


import pandas as pd
import requests

LOGGER = logging.getLogger("extract_eco2mix")

ORDRE_BASE_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
PAGE_SIZE = 100 
MAX_RETRIES = 5
RETRY_BACKOFF_SECONDS = 5


def fetch_all_records(dataset_id: str, order_by: str ="date_heure", where: str | None = None) -> pd.DataFrame:
    """ recupère l'intégralité d'un datase ODRÉ avec pagination"""
    
    url = f"{ORDRE_BASE_URL}/{dataset_id}/records"
    all_records : list[dict] = []
    offset = 0
    total_count = None
    
    while total_count is None or offset < total_count:
        params = {
            "limit": PAGE_SIZE,
            "offset": offset,
            "order_by": order_by
        }
        if where:
            params["where"] = where
        
        payload = _get_with_retries(url, params=params)
        total_count = payload.get("total_count", 0)
        results = payload.get("results", [])
        
        if not results:
            LOGGER.info("Plus de resultats à récupérer, arrêt de la pagination.", offset=offset)
            break
        
        all_records.extend(results)
        LOGGER.info(
            "Récupération des enregistrements (offset=%s): %d/%d pour le dataset %s ", offset, len(all_records), total_count, dataset_id,
            
        )
        
        offset += PAGE_SIZE
        
    return pd.DataFrame(all_records)


def _get_with_retries(url: str, params: dict) -> dict:
    """
        Appelle l'API avec quelques tentatives en cas d'échec
                        """ 
    
    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 429:
                wait = RETRY_BACKOFF_SECONDS * attempt
                LOGGER.warning("Trop de requêtes (429). Attente de %d secondes avant la prochaine tentative.", wait)
                time.sleep(wait)
                continue   
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            last_exception = e
            wait = RETRY_BACKOFF_SECONDS * attempt
            LOGGER.warning("Erreur lors de l'appel à l'API (tentative %d/%d): %s. Attente de %d secondes avant la prochaine tentative.", attempt, MAX_RETRIES, e, wait)
            time.sleep(wait)
            
    raise RuntimeError(f"Échec de l'appel à l'API après {MAX_RETRIES} tentatives: {last_exception}")





def write_local(df: pd.DataFrame, output_dir: str, dataset_id: str) -> Path:
    """Ecrit le dataframe dans un fichier local"""
    out_dir = Path(output_dir) 
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = out_dir / f"{dataset_id}_{timestamp}.parquet"
    
    df.to_parquet(output_path, index=False)
    LOGGER.info("Données écrites dans le fichier local: %s", output_path)
    return output_path




def write_bigquery ( df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str) -> None:
    """Ecrit le dataframe dans une table BigQuery, en mode append"""
    from google.cloud import bigquery
    
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    
    df = df.copy()
    df["_extracted_at"] = datetime.now(timezone.utc)
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )
    
    
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()  # Attendre la fin du job
    LOGGER.info("Données écrites dans la table BigQuery: %s.%s.%s, nbre de lignes chargées : %s", project_id, dataset_id, table_id, len(df))
    
    
def parse_args() -> argparse.Namespace:
    
    parser = argparse.ArgumentParser(description="Extraction des données éCO2mix (RTE/ODRÉ).")
    parser.add_argument(
        "--dataset",
        default="eco2mix-national-tr",
        help="Identifiant du dataset ODRÉ (défaut : eco2mix-national-tr).",
    )
    parser.add_argument(
        "--where",
        default=None,
        help="Filtre optionnel au format Opendatasoft, ex: \"date_heure > date'2026-08-01'\".",
    )
    parser.add_argument(
        "--dest",
        choices=["local", "bigquery"],
        default="local",
        help="Destination de l'écriture : fichier local (Parquet) ou BigQuery.",
    )
    parser.add_argument("--output-dir", default="./data", help="Répertoire de sortie si --dest local.")
    parser.add_argument("--bq-project", default=None, help="Project ID GCP si --dest bigquery.")
    parser.add_argument("--bq-dataset", default="raw", help="Dataset BigQuery cible.")
    parser.add_argument("--bq-table", default="eco2mix_national", help="Table BigQuery cible.")
    parser.add_argument("--verbose", action="store_true", help="Active les logs de niveau DEBUG.")
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )
    
    LOGGER.info("Début de l'extraction des données éCO2mix pour le dataset %s", args.dataset)
    
    df = fetch_all_records(args.dataset, where=args.where)
    
    if df.empty:
        LOGGER.warning("Aucun enregistrement récupéré pour le dataset %s avec le filtre '%s'.", args.dataset, args.where)
        return
    
    if args.dest == "local":
        write_local(df, args.output_dir, args.dataset)
    elif args.dest == "bigquery":
        if not args.bq_project:
            LOGGER.error("Le paramètre --bq-project est requis pour l'écriture dans BigQuery.")
            sys.exit(1)
        write_bigquery(df, args.bq_project, args.bq_dataset, args.bq_table)
    
    LOGGER.info("Extraction terminée avec succès.")
    

if __name__ == "__main__":
    sys.exit(main())