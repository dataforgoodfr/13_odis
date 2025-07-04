# OD&IS

Created: March 5, 2025 5:54 PM
type: documentation

DataForGood saison 13. OD&IS est une application permettant à l’association J’Accueille de visualiser des données utiles à l’insertion et l’implantation de personnes en situation de migration, vers des territoires adaptés à leurs besoins en termes de logement, santé, services publics, éducation etc…

Le but de ce projet est de récupérer et traiter les différentes sources de données utiles, de façon fiable, répétable et régulièrement mise à jour.

# Installation et configuration du projet

Pour prendre en main ce repository et installer les dépendances du projet :
- [Guide d'installation](./INSTALL.md)

## Variables d'environnement

Pour créer le fichier local des variables d'environnement (.env) à partir de .env.dist :

```bash
cp .env.dist .env
```

Ajouter les secrets S3 dans le fichier .env.

## Base de données

Ce projet utilise une base de données PostgreSQL gérée via Docker Compose. 
Pour démarrer la base de données :

```bash
docker compose up -d
```
Ou bien utilisez Docker Desktop et activez le container (après avoir run cp .env.dist .env et docker compose up -d une première fois)

La base de données sera ensuite accessible sur `localhost:5432` et le pgadmin sur :`localhost:5050` 

Pour initialiser ou réinitialiser toute la base de données :

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
poetry run bin/odis.py extract --domain geographical_references

# Extraire seulement les datasets "regions" et "departements du domaine "geographical_references"
poetry run bin/odis.py extract --sources geographical_references.regions, geographical_references.departements
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
poetry run bin/odis.py explain --api INSEE.Melodi,INSEE.Metadonneees

# Voir les définitions détaillées d'une source de données et de son API
poetry run bin/odis.py explain --api DiDo --source logement.dido_catalogue 
```

## Chargement des données brutes

La fonction load permet de charger un fichier local dans la base de données.

```bash
poetry run bin/odis.py load -s logement.logements_maison
```

## Télécharger la méthodologie et les modèles de données cibles

Il s'agit d'une méthodologie reprenant les opérations de transformation effectuées (manuellement) depuis les données sources. Puis d'un ensemble de csv qui correspondent aux données transformées cibles.

```bash
poetry run python ./common/utils/download_target_data.py

```

## Lancer le projet DBT

### Installation de dbt

```yaml
# Vérifier l’installation effectuée avec Poetry install
dbt --version

# Sinon pour réinstaller
pip install dbt-core
# Adapter pour PostgreSQL
pip install dbt-postgres
```

### Rentrer sur le dossier DBT pour continuer l'init sur projet et commencer à travailler
(pour reconnaître votre dbt_project.yml)

```yaml
# Sous_dossier su projet DBT
cd dbt_odis

# Installer les dépendances et packages
dbt deps

# Vérifier l'intégrité du projet
dbt debug
```

La première fois, DBT core n'est pas forcément reconnu, relancez VScode

### Installez l'extension Power User for DBT sur VScode

Cela permettra de tester vos requêtes SQL sans matérialiser une table dans Postgre

### Premières commandes DBT, ou utilisez l'extension Power User
```yaml
# Si vous n'avez pas encore loadé vos sources bronze, retournez sur la racine du projet
cd -

# Revenez sur le dossier dbt
cd dbt_odis

# Si vous avez besoin du fichier de correspondances Codes Postaux - Codes INSEE
dbt seed

# Construisez vos premiers models
dbt build --select model 'nom_de_votre_model'

```

En utilisant Power User, vous n'avez pas besoin d'être sur dbt_odis, l'extension ouvre un terminal dédiée pour vos commandes DBT
- Bouton Play en blanc = requête SQL sur tout le model ou seulement les lignes que vous sélectionnez
- Bouton Bonhomme = dbt run
- Bouton Check = dbt test
- Bouton Marteau = différentes options de dbt build

### Toutes les commandes DBT intégrées directement dans la CI ici : 

- [Commandes DBT](./docs/DBT.md)

## Méthodologie Médaillon et Raisonnement DBT

Adoption d'une architecture médaillon sur DBT
- Bronze = staging et transformation des sources en tabulaire
- Silver = intermediate : normalisation des données, unions et enrrichissement
- Gold = calculs d'indicateurs et aggrégations demandées, sélection pour Tables WDD

### Raisonnement Structure Postgre / Relationnel DBT

* Bronze_Table = Source loadée
    * JSON 1 colonne data
    * CSV (sources xlsx, csv, zip)

* Bronze_View = Extraction de la source 
    * from {{ source ( 'bronze' , 'nom_de_source' ) }}
    * Parsing de Json en Jinja : (data::jsonb) → 'sous_niveau' ->> 'valeur'
    * Select all de CSV : format variable en Macros dbt_utils.star ou dbt_utils.get_filtered_columns_in_relation

* Silver_View (si besoin d'un layer) ou Silver_Table
    * from {{ ref ( 'nom_de_model_bronze' ) }}
    * 1 seule table finale silver : avoir toutes les données disponibles pour le futur agent IA recherche inversée
    * join de toutes les Bronze_View
    * enrichissement avec les zones géographiques, code_INSEE (dispos dans geographical_references)
    * union all des 3 zones géographiques communes-départements-régions
    * union all des 3 types de zones communes-départements-régions
    * macro pivot si besoin ->  avoir les colonnes temporelles en variable
    * reformater les datatypes si besoin en integer (attention absolument garder les codegeo en texte)

* Gold_Table
    * from {{ ref ( 'nom_de_model_silver' ) }}
    * Adhérer aux tables de destination demandées par WDD
    * Select des colonnes nécessaires
    * Renommage comme WDD : champ as "Nouveau_Nom" (pour garder les majuscules et accents)


En têtes des models à utiliser :

```sql
{{ config(
    tags = ['bronze/silver/gold', 'domaine_du_model'],
    alias = 'nom_de_la_table/view_dans_postgre'
    )
}}
```

