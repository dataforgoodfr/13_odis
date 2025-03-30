# OD&IS

Created: March 5, 2025 5:54 PM
type: documentation

DataForGood saison 13. OD&IS est une application permettant à l’association J’Accueille de visualiser des données utiles à l’insertion et l’implantation de personnes en situation de migration, vers des territoires adaptés à leurs besoins en termes de logement, santé, services publics, éducation etc…

Le but de ce projet est de récupérer et traiter les différentes sources de données utiles, de façon fiable, répétable et régulièrement mise à jour.

# Installation

Pour prendre en main ce repository et installer les dépendances du projet :
- [Guide d'installation](./INSTALL.md)

```bash
cp .env.dist .env
poetry install
```

Ce projet utilise une base de données PostgreSQL. Pour démarrer la base de données en local :

```bash
docker compose up -d
```

La base de données sera ensuite accessible sur `localhost:5432`

Pour initialiser ou réinitialiser la base de données :

```bash
poetry run python bin/db.py init
```

# Approche : ELT + architecture médaillon

Pour récupérer les données et les modeler selon les besoins de J’Accueille, ce projet implémente une approche ELT : Extract-Load-Transform.

L’architecture de données est dite en "médaillon" avec trois niveaux de “maturité” des données : bronze (données brutes), silver (données nettoyées, recoupées) et gold (données traitées prêtes à l'emploi pour l’application). 

Une approche ELT avec une architecture de données en médaillon, ça donne donc la suite de tâches suivante :

1. On extrait les données OpenData de toutes nos sources intéressantes ( = Extract )
2. On charge toutes ces données brutes dans la couche “bronze” de la base ( = Load )
3. On effectue toutes les transformations nécessaires pour aller de la couche Bronze à la Silver puis la Gold ( = Transform )

# Utilisation

## Extraction

L’extraction de données se fait grâce à des Extracteurs (des classes Python), qui se basent sur le fichier de configuration “datasources.yaml”, et appellent les différentes API ciblées.

Le fichier “datasources.yaml” répertorie toutes nos sources de données et est organisé autour de trois grandes notions :

- Les API sur lesquelles on va chercher la donnée ( APIs INSEE, API du ministère du logement, etc…)
- Les “Domaines” de donnée qui regroupent les jeux de données en thématiques : géographie, logement, emploi etc
- Au sein de chaque Domaine, des “Sources” qui représentent les informations sur les jeux de données précis à récupérer

Le script “extract.py” permet de récupérer des jeux de données en ligne de commande, en choisissant un Domaine, et un ou plusieurs Sources. Par défaut, si on ne précise pas de Source, les données sont extraites pour toutes les Sources définies pour le Domaine choisi.

```bash
# Extraire tous les datasets source du domaine "geographical_references"
poetry run python bin/odis.py extract --domain geographical_references

# Extraire seulement les datasets "regions" et "departements du domaine "geographical_references"
poetry run python bin/extract.py --sources geographical_references.regions geographical_references.departements
```

Pour comprendre en détail comment ça fonctionne : 

[Comprendre la configuration déclarative des sources de données](./docs/configurations.md)

[Comprendre les Extracteurs de données](./docs/extract.md)

## Sonder les sources disponibles

L’option “explain” permet de voir facilement comment les API, Domaines et Sources sont définis dans la configuration. Si l’option “explain” est passée, le script n’extrait aucune donnée mais montre seulement les infos sur les configurations demandées.

```bash
# Voir la liste des API, domaines et sources disponibles
poetry run bin/odis.py explain

# Voir les définitions de tous les datasets source du domaine "geographical_references"
poetry run bin/odis.py explain --domain geographical_references 

# Voir les définitions détaillées de l'API DiDo
poetry run bin/odis.py explain --api DiDo

# Voir les définitions détaillées de plusieurs API INSEE
poetry run bin/odis.py explain --api INSEE.Melodi INSEE.Metadonneees

# Voir les définitions détaillées d'une source de données et de son API
poetry run bin/odis.py explain --api DiDo --source logement.dido_catalogue 
```

## Chargement des données brutes

La fonction load permet de charger un fichier local dans la base de données.

```bash
poetry run python bin/odis.py load -s logement.dido_catalogue
```

## Télécharger la méthodologie et les modèles de données cibles

Il s'agit d'une méthodologie reprenant les opérations de transformation effectuées (manuellement) depuis les données sources. Puis d'un ensemble de csv qui correspondent aux données transformées cibles.

```bash
poetry run python ./common/utils/download_target_data.py

```

## Lancer le projet DBT

### Installation de dbt

```yaml
pip install dbt-core
# Adapter pour PostgreSQL
pip install dbt-postgres
# Vérifier l’installation
dbt --version
```

### Installation des dépendances

```yaml
dbt deps
```

### **Se placer sur le dossier DBT pour commencer à travailler**

(pour reconnaître votre dbt_project.yml)

```yaml
cd dbt_odis
```

Toutes les commandes DBT intégrées directement dans la CI ici : 

- [Commandes DBT](./docs/DBT.md)