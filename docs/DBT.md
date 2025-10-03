# DBT (data build tool)

## Introduction

dbt (Data Build Tool) est un outil de transformation de données qui permet aux équipes d’analyser et de modéliser leurs données en appliquant des bonnes pratiques de développement logiciel, telles que la modularité, la portabilité, l’intégration continue (CI/CD) et la documentation. Il permet aux analystes et ingénieurs de transformer les données directement dans leur entrepôt de données en écrivant des requêtes SQL modulaires.

## Concepts Clés

### Models

Les modèles sont des fichiers SQL qui transforment vos données. Chaque modèle :

- Correspond à un fichier SQL unique dans votre projet
- Contient des sous-requêtes avec WITH AS pour préparer la/les jointure(s), puis une seule requête SELECT (une seule table/vue créée par model)
- Crée une table ou une vue dans l'entrepôt de données

```sql
-- Example model: models/bronze/geographical_references_communes.sql : extraction du JSON en code Jinja_sql
with communes as 
(
    select 
        id, 
        (data::jsonb) ->> 'nom' as nom, 
        (data::jsonb) ->> 'code' as code, 
        (data::jsonb) -> 'centre' ->> 'type' as geo_type,
        (data::jsonb) -> 'centre' -> 'coordinates'->> 0 as geo_coordonnees_longitude,
        (data::jsonb) -> 'centre' -> 'coordinates'->> 1 as geo_coordonnees_lattitude,
        (data::jsonb) -> 'region' ->> 'nom' as region_nom,
        (data::jsonb) -> 'region' ->> 'code' as region_code,
        (data::jsonb) -> 'departement' ->> 'nom' as departement_nom,
        (data::jsonb) -> 'departement' ->> 'code' as departement_code,
        (data::jsonb) -> 'population' as population,        
        created_at
    from {{ source('bronze', 'geographical_references_communes') }} 
)

select * from communes
```

### Sources

Les sources définissent les données brutes qui sont utilisées dans votre projet dbt. Elles permettent de créer un graphe de dépendances et rendent vos requêtes SQL plus lisibles.

```yaml
# models/bronze/_odis_bronze__sources.yml
version: 2

sources:
  - name: bronze
    schema: bronze
    tables:
      - name: geographical_references_communes
        description: Source JSON loadée dans le champ data contenant les références des communes
        loaded_at_field: created_at
```

### Tests

dbt permet de tester vos modèles pour assurer la qualité des données :

```yaml
# models/bronze/_odis_bronze__models
version: 2

models:
  - name: geographical_references_communes
    columns:
      - name: nom
        description: Nom des communes avec articles et tirets 
      - name: code
        description: Code officiel français des communes, string pour garder 01...
        tests:
          - unique
          - not_null
      - name: geo_type
        description: Type de coordonnées géographiques
        tests:
          - accepted_values:
              values: ['Point']
```

### Documentation des models

Power User for Dbt génère une documentation interactive pour votre projet, facilitant la compréhension des modèles de données.

Une fois que vous avez run votre model une première fois
Commencez par renseigner le nom et la description de votre table issue du model dans le couche_models.yml
Power User va détecter l'emplacement de la table/view dans Postgre et de sa documentation, puis synchroniser

```yaml
# models/bronze/_odis_bronze__models
  - name: logement_logements_sociaux_departement
    description: CSV contenant les logements sociaux déclarés par départements
```

Ensuite revenez sur votre model, allez sur la barre d'outils de votre terminal, onglet Documentation Editor
Vous retrouvez votre description
Synchronisez avec la base de donnée (bouton jaune)
Vous découvrez vos colonnes, renseignez leur description pour chaque
(vous pouvez ajouter des tests)
Vérifiez vos data_types
Bouton 'Save', Power User réécrit le models.yml directement

## Structure du projet

Une structure classique d'un projet dbt ressemble à ceci :

```
dbt_project/
├── dbt_project.yml          # Configuration du projet
├── profiles.yml             # Configuration de la connexion à la base de données
├── models/                  # Fichiers SQL de transformation
│   ├── staging/             # Modèles de pré-traitement (couche BRONZE)
│   ├── intermediate/        # Modèles intermédiaires (couche SILVER)
│   └── marts/               # Modèles business (couche GOLD)
├── tests/                   # Tests de qualité des données
├── macros/                  # Fonctions SQL réutilisables
├── snapshots/               # Historisation des données
├── analysis/                # Analyses ponctuelles
└── seeds/                   # Fichiers CSV injectés dans la base de données
```

## Utiliser dbt

### Installation de dbt

```bash
pip install dbt-core
# Adapter pour PostgreSQL
pip install dbt-postgres
# Vérifier l’installation
dbt --version
```

### Installation des dépendances

```bash
dbt deps
```

### Run des modèles - Crée des Tables ou des Vues dans l'entrepôt de données, sans appliquer les tests

```bash
# Exécuter tous les modèles
dbt run

# Exécuter un modèle spécifique
dbt run --models model_name

# Exécuter les modèles d’un dossier spécifique (ex: bronze)
dbt run --select bronze

# Exécuter des modèles avec un tag spécifique
dbt run --models tag:emploi

# Exécuter des modèles sur un environnement spécifique (ex: dev_live)
dbt run --models --target dev_live
```
Vous avez un environnement de test local si vous le souhaitez : --target dev_custom
L'environnement de construction classique est --target dev_live

### Test des Modèles - Vérifie les tests écrits dans les /schema.yml

```bash
# Exécuter tous les tests
dbt test

# Tester un modèle spécifique
dbt test --models model_name
```

### Build des modèles - Exécution simultanée des commandes dbt run, dbt test et dbt seed. Attention : si un test échoue, dbt ne construira pas les tables ou vues des modèles dépendants du modèle en échec.

```bash
# Construire tous les modèles
dbt build

# Construire un modèle et ses dépendances/modèles enfants
dbt build --select model_name+

# Construire un modèle et ses racines/modèles parents
dbt build --select +model_name

# Construire avec un rafraîchissement complet des tables et vues
dbt build --full-refresh
```

### Génération de la documentation globale en JSON

```bash
# Créer un fichier JSON recensant toutes les tables, colonnes et connexions de votre projet DBT
# Path : 13_odis/dbt_odis/target/catalog.json
dbt docs generate

# Lancer une page web en Localhost montrant toutes les informations de votre projet DBT, CTRL+C pour arrêter la webpage
dbt docs serve
```

### Nettoyage des fichiers temporaires et des packages

```bash
dbt clean
# Veiller à refaire la commande dbt deps ensuite
```

## Configuration

### dbt_project.yml

Fichier de configuration du projet dbt :

```yaml
name: "dbt_odis"
version: "1.0.0"

# This setting configures which "profile" dbt uses for this project.
profile: "dbt_odis"

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets: # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"

models:
  dbt_odis:
    # Config indicated by + and applies to all files under models/example/
    bronze:
      +schema: bronze
      +materialized: view
```

### profiles.yml

Fichier de configuration de la connexion à l’entrepôt de données :

```yaml
dbt_odis:
  target: dev_live
  outputs:
    dev_live:
      dbname: odis
      host: localhost
      pass: odis
      port: 5432
      schema: bronze
      threads: 16
      type: postgres
      user: odis

    dev_custom:
      dbname: odis
      host: localhost
      pass: odis
      port: 5432
      schema: 'vos_initiales'
      threads: 16
      type: postgres
      user: odis
```

## Bonnes pratiques

1. **Adopter une convention de nommage claire** pour les modèles et les colonnes
2. **Organiser les modèles en couches** (BRONZE, SILVER, GOLD)
3. **Tester systématiquement les modèles** pour garantir la qualité des données
4. **Documenter les modèles** afin d'améliorer la compréhension du projet
5. **Surveiller la fraîcheur des sources** pour détecter les données obsolètes
6. **Mettre en place un pipeline CI/CD** pour automatiser les tests et les déploiements

## Documentation officielle et ressources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Learn](https://courses.getdbt.com/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [dbt GitHub Repository](https://github.com/dbt-labs/dbt-core)
- [dbt Slack Community](https://www.getdbt.com/community/join-the-community/)
- [dbt Blog](https://blog.getdbt.com/)

## Approfondissements

- [dbt Package Hub](https://hub.getdbt.com/)
- [dbt Cloud](https://docs.getdbt.com/docs/dbt-cloud/cloud-overview)
- [dbt APIs](https://docs.getdbt.com/docs/dbt-cloud/dbt-cloud-api)
- [Incremental Models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/configuring-incremental-models)
- [Snapshots](https://docs.getdbt.com/docs/building-a-dbt-project/snapshots)
- [Hooks](https://docs.getdbt.com/docs/building-a-dbt-project/hooks-operations)
