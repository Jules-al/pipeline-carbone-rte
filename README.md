## pipeline-carbone-rte : Quand consommer pour réduire son empreinte carbone

Pipeline ELT qui suit l'intensité carbone du réseau électrique français en temps réel, pour identifier les meilleures heures pour consommer de l'électricité (recharge véhicule, électroménager, pilotage énergétique).
 
## Le problème
 
Le mix électrique français varie fortement selon l'heure (nucléaire, éolien, solaire, gaz), donc l'intensité carbone de l'électricité aussi. Ce projet a pour but final de transformer la donnée brute publiée par RTE en un signal simple : à quel moment consommer pour minimiser son empreinte carbone.
 
 ## Architecture
 

API RTE éCO2mix (ODRÉ)
        &darr
  Extraction (Python)
        &darr
  BigQuery (raw)
        &darr
  dbt (staging → intermediate → marts)
        &darr
  Dashboard (Looker Studio)


## Stack
 
- Extraction : Python (requests, pandas)
- Stockage : Google BigQuery
- Transformation : dbt
- Restitution : Looker Studio
- Source de données : [Open Data RTE / ODRÉ](https://opendata.reseaux-energies.fr) (API publique, sans authentification)
## Utilisation
 
```bash
pip install -r extraction/requirements.txt
 
# Test en local (fichier Parquet, sans BigQuery)
python extraction/extract_eco2mix.py --dest local --output-dir ./data
 
# Extraction vers BigQuery
python extraction/extract_eco2mix.py --dest bigquery \
    --bq-project <mon-projet-gcp> \
    --bq-dataset raw \
    --bq-table eco2mix_national
```
 
## État du projet
 
- [x] Script d'extraction (API RTE → local / BigQuery)
- [ ] Modélisation dbt (staging, intermediate, marts)
- [ ] Dashboard Looker Studio
- [ ] Orchestration automatisée
## Auteur
 
Jules Kepsu | DS/DA | Applied Mathematics Engineer | AI Enthusiast



