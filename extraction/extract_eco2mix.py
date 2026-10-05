import argparse
import logging 
import sys 
import time 
from datetime import datetime, timezone
from pathlib import Path


import pandas as pd
import requests

LOGGER = logging.getLogger("extract_eco2mix")

ODRE_BASE_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
EXPORT_TIMEOUT_SECONDS = 300 
MAX_RETRIES = 5
RETRY_BACKOFF_SECONDS = 5


def fetch_all_records(dataset_id: str, order_by: str = "date_heure", where: str | None = None) -> pd.DataFrame:
    """Récupère le dataset via l'endpoint d'export (pas de limite des 10 000 lignes)."""
    url = f"{ODRE_BASE_URL}/{dataset_id}/exports/json"
    params = {"order_by": order_by}
    if where:
        params["where"] = where

    records = _get_with_retries(url, params=params, timeout=EXPORT_TIMEOUT_SECONDS)
    LOGGER.info("%d enregistrements récupérés pour le dataset %s", len(records), dataset_id)

    df = pd.DataFrame(records)
    if df.empty:
        return df

    # Typage explicite : BigQuery créera une colonne TIMESTAMP (et non STRING)
    df["date_heure"] = pd.to_datetime(df["date_heure"], utc=True)
    # Le dataset temps réel contient aussi les quarts d'heure à venir (prévisions, mesures vides) :
    # on ne garde que les lignes déjà mesurées, sinon elles ne seraient jamais complétées en incrémental
    df = df.dropna(subset=["taux_co2"])
    df = df.drop_duplicates(subset="date_heure").sort_values("date_heure")
    return df


def _get_with_retries(url: str, params: dict, timeout: int = 30) -> dict | list:
    """
        Appelle l'API avec quelques tentatives en cas d'échec
                        """ 
    
    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
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


def get_last_timestamp_bigquery(project_id: str, dataset_id: str, table_id: str) -> datetime | None:
    """Renvoie le date_heure le plus récent déjà chargé, ou None si la table n'existe pas."""
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    query = f"SELECT MAX(date_heure) AS last_ts FROM `{project_id}.{dataset_id}.{table_id}`"
    try:
        rows = list(client.query(query).result())
    except NotFound:
        LOGGER.info("Table %s.%s.%s inexistante : extraction complète.", project_id, dataset_id, table_id)
        return None
    return rows[0].last_ts


def get_last_timestamp_local(output_dir: str, dataset_id: str) -> datetime | None:
    """Renvoie le date_heure le plus récent parmi les fichiers Parquet déjà écrits."""
    files = list(Path(output_dir).glob(f"{dataset_id}_*.parquet"))
    if not files:
        return None
    last_ts = max(pd.read_parquet(f, columns=["date_heure"])["date_heure"].max() for f in files)
    return last_ts.to_pydatetime()


def build_where(user_where: str | None, last_ts: datetime | None) -> str | None:
    """Combine le filtre utilisateur et le filtre incrémental en une clause ODSQL."""
    clauses = []
    if user_where:
        clauses.append(f"({user_where})")
    if last_ts is not None:
        ts = last_ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        clauses.append(f"date_heure > date'{ts}'")
    return " AND ".join(clauses) or None


def write_local(df: pd.DataFrame, output_dir: str, dataset_id: str) -> Path:
    """Ecrit le dataframe dans un fichier local"""
    out_dir = Path(output_dir) 
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = out_dir / f"{dataset_id}_{timestamp}.parquet"
    
    df.to_parquet(output_path, index=False)
    LOGGER.info("Données écrites dans le fichier local: %s", output_path)
    return output_path




def write_bigquery(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str, full_refresh: bool = False) -> None:
    """Écrit le dataframe dans BigQuery (append, ou écrasement si full_refresh)."""
    from google.cloud import bigquery
    
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    df = df.copy()
    df["_extracted_at"] = datetime.now(timezone.utc)
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE if full_refresh
            else bigquery.WriteDisposition.WRITE_APPEND
        ),
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
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore les données existantes et recharge tout (écrase la table BigQuery).",
    )
    parser.add_argument("--verbose", action="store_true", help="Active les logs de niveau DEBUG.")
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )
    
    if args.dest == "bigquery" and not args.bq_project:
        LOGGER.error("Le paramètre --bq-project est requis pour l'écriture dans BigQuery.")
        sys.exit(1)

    LOGGER.info("Début de l'extraction des données éCO2mix pour le dataset %s", args.dataset)

    last_ts = None
    if not args.full_refresh:
        if args.dest == "bigquery":
            last_ts = get_last_timestamp_bigquery(args.bq_project, args.bq_dataset, args.bq_table)
        else:
            last_ts = get_last_timestamp_local(args.output_dir, args.dataset)

    if last_ts is not None:
        LOGGER.info("Mode incrémental : récupération des données après %s", last_ts)

    where = build_where(args.where, last_ts)
    df = fetch_all_records(args.dataset, where=where)

    if df.empty:
        LOGGER.info("Aucune nouvelle donnée à charger.")
        return

    if args.dest == "local":
        write_local(df, args.output_dir, args.dataset)
    else:
        write_bigquery(df, args.bq_project, args.bq_dataset, args.bq_table, full_refresh=args.full_refresh)

    LOGGER.info("Extraction terminée avec succès.")


if __name__ == "__main__":
    sys.exit(main())