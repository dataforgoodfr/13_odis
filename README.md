# OD&IS

À compléter...

DataForGood saison 13.

## Installation

Pour prendre en main ce repository et installer les dépendances du projet :
- [Guide d'installation](./INSTALL.md)

```bash
cp .env.dist .env
poetry install
```

## Pour démarrer la base de données en local

```bash
docker compose up -d
```

La base de données sera ensuite accessible sur `localhost:5432`

## Pour initialiser ou réinitialiser la base de données

```
poetry run python bin/db.py init
```

## Pipeline ELT

### Approche

Ce projet utilise une architecture en "médaillon" avec trois niveaux de traitement des données : bronze (données brutes), silver (données nettoyées) et gold (données traitées prêtes à l'emploi). Les sources de données sont classées par domaines dans le fichier `datasources.yaml`. Des scripts dédiés gèrent l'extraction et le chargement des données dans la base, tandis que les transformations entre niveaux sont effectuées via DBT.

### Extraction

```bash
poetry run python bin/extract.py --domain geographical_references
```

### Chargement des données bruts

```bash
poetry run python bin/load.py --domain geographical_references
```


## Télécharger la méthodologie et les modèles de données cibles

Il s'agit d'une méthodologie reprenant les opérations de transformation effectuées (manuellement) depuis les données sources. Puis d'un ensemble de csv qui correspondent aux données transformées cibles.

```bash
poetry run python ./common/utils/download_target_data.py
```
