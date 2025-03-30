# Configuration déclarative des sources

Created: March 5, 2025 10:57 PM
type: documentation

Le fichier `datasources.yaml` répertorie toutes nos sources de données et est organisé autour de trois grandes notions :

- Les API sur lesquelles on va chercher la donnée ( APIs INSEE, API du ministère du logement, etc…)
- Les “Domaines” de donnée qui regroupent les jeux de données en thématiques : géographie, logement, emploi etc
- Au sein de chaque Domaine, des “Sources” qui représentent les informations sur les jeux de données précis à récupérer

Les définitions sont données dans un fichier yaml pour être facilement lisibles et extensibles. Le fichier est organisé en deux grands blocs : définitions des APIs, et définitions des sources de données.

# Comment ajouter des nouvelles sources

Pour ajouter des nouveaux dataset sources à extraire, il faut donc en prérequis, avoir exploré et identifié quelles API et quelles jeux de données sont intéressants, et avoir compris comment lesdites API fonctionnent.

Une fois que vous savez ce que vous voulez récupérer, il faut ajouter une ou deux choses au code :

1. Créer une nouvelle branche “feat” à partir de “main”, et associée à une Task dans le board Github
2. Ajouter les déclarations d’API, domaine et sources dans `datasources.yaml` , selon le format décrit plus bas
3. Si aucun des Extractors existants dans `common/utils/source_extractors.py` ne convient aux besoins et contraintes de l’API que vous ciblez, vous pouvez soit modifier un Extractor existant. Pour voir comment faire en détail : [Extractors](./extract.md)
4. Tester en local, et une fois que ça marche pour vous, soumettre une PR 

Good luck 🙂

# Définition des API

Les API sont définies dans le bloc `APIs` :

```yaml
APIs:

  INSEE.Metadonnees:
    name: Metadonnees INSEE
    description: INSEE - API des métadonnées
    base_url: https://api.insee.fr/metadonnees/V1
    apidoc: https://api.insee.fr/catalogue/site/themes/wso2/subthemes/insee/pages/item-info.jag?name=M%C3%A9tadonn%C3%A9es&version=V1&provider=insee
    default_headers:
        accept: application/json
```

Un bloc de définition d’une API comporte obligatoirement: 

- `name` et `description` : Des informations générales destinées à faciliter la lecture et la compréhension par des humains
- `apidoc` : Un lien vers la documentation technique de l’API (le swagger par exemple)
- `base_url` : L’URL à la base de tous les appels
- `throttle` : Le nombre de requêtes par minute à ne pas dépasser avec cette API (en général imposé par le fournisseur de l’API)

Optionnellement, un bloc API peut déclarer toute information utile pour prendre en compte les contraintes de l’API : 

- `default_headers` : headers par défaut à passer (par exemple “application/json” ou “text/csv” …)
- paramètres obligatoires, etc…

# Définition des modèles Sources de données

Les modèles sources permettent de définir :

- Les informations nécessaires pour extraire la donnée des sources : quelle API, quel endpoint, comment extraire…
- Les informations nécessaires pour charger les données extraites dans la base de données Bronze : quel format de données récupérer, comment le retraiter avant insertion si besoin

Les jeux de données Source sont organisées par domaine, dans le bloc `domains` :

```yaml
domains:

  geographical_references:

    regions:
      API: INSEE.Metadonnees
      description: Référentiel géographique INSEE - niveau régional
      type: JsonExtractor
      endpoint: /geo/regions

    departements:
      API: INSEE.Metadonnees
      description: Référentiel géographique INSEE - niveau départemental
      type: JsonExtractor
      endpoint: /geo/departements
```

Dans l’exemple ci-dessus, est déclaré le domaine “geographical_references”, qui contient les modèles source pour les jeux de donnée “régions” et “départements” du référentiel géographique de l’INSEE.

## Champs obligatoires

**Un bloc “source” définit obligatoirement les champs suivants :**

- `description` : description claire et concise pour aider à la compréhension
- `type` : quel type d’Extracteur doit être utilisé pour récupérer ce dataset
- `format` : quel est le format de fichier attendu : doit être `json`, `csv`, ou `xlsx`

Dans l’exemple donné, pour récupérer le dataset “regions”, un Extracteur de classe “JsonExtractor” sera donc instancié, pour requêter l’API INSEE.Metadonnees sur l’URL complète suivante :

`https://api.insee.fr/metadonnees/v1/geo/regions`

## Champs facultatifs

Un bloc “source” peut définir les champs optionnels suivants :
- `API` : quelle API est à la source de ce dataset
- `endpoint` : comment l’URL de l’API doit être complétée pour requêter ce jeu de données
- `notebook_path` : le chemin relatif (depuis la racine du projet) du notebook

### Paramètres http pour la requête

`extract_params` est un dictionnaire qui définit les paramètres http à passer dans la requête. Définir un champ `extract_params` est équivalent à mettre les paramètres dans le champ `endpoint` avec la syntaxe URL http classique.

Par exemple, ceci :

```yaml
endpoint: /domain/model
extract_params:
	scope: FR
	annual_data: 2023

```

Est équivalent à ceci :

```yaml
endpoint: /data/model?scope=FR&annual_data=2024
```

Le Path a priorité sur le champ `extract_params` : Si une configuration définit à la fois une querystring dans le champ `endpoint` et un champ `extract_params`, le contenu de `extract_params` est ignoré dans la construction de la requête.


### Mapping de la réponse

`response_map` est un dictionnaire qui indique comment trouver, dans le corps de la réponse, toute information intéressante. 

Les valeurs données dans les champs de `response_map` permettent au code python de trouver des valeurs dans un JSON en utilisant la syntaxe JMESPath.

Cela permet notamment de gérer la pagination, ou le loading en base plus efficacement. L’idée est de trouver les infos dont on a besoin sans avoir à enchaîner des dict[”key1”][”key2”] etc…

Exemple : 

Si dans une réponse JSON qui ressemble à ça : 

```json
{
  "identifier": "DS_IPCH_A",
  "title": {
    "fr": "Indice des prix à la consommation harmonisés annuels",
    "en": "Harmonised Indices of Consumption Prices (IHCP) - annual"
  },
  "observations": [
    {
      "measures": {
        "OBS_VALUE_INDICE_DE_PRIX": {
          "value": 95.28
        }
      }
    },
    {
      [etc...]
    }
  ]
  ],
  "paging": {
    "first": "https://api.insee.fr/melodi/data/DS_IPCH_A?page=1&maxResult=20&totalCount=true&startPeriod=2020-01-01&endPeriod=2021-01-01&idObservation=true&range=true&idTerritoire=true&includeHistory=true",
    "next": "https://api.insee.fr/melodi/data/DS_IPCH_A?page=2&maxResult=20&totalCount=true&startPeriod=2020-01-01&endPeriod=2021-01-01&idObservation=true&range=true&idTerritoire=true&includeHistory=true",
    "last": "https://api.insee.fr/melodi/data/DS_IPCH_A?page=42&maxResult=20&totalCount=true&startPeriod=2020-01-01&endPeriod=2021-01-01&idObservation=true&range=true&idTerritoire=true&includeHistory=true",
    "isLast": false,
    "count": 830
  }
}
```

Je veux récupérer la valeur de “paging.next” pour paginer, je définis : 

```yaml
response_map:
	next: paging.next
```

Cela permet de récupérer en utilisant la fonction python `jmespath.search`

Plus de détails sur la syntaxe JMESPath ici :

- Quelques exemples pour vite comprendre : https://jmespath.org/examples.html
- Playground pour tester facilement : https://jmespath.org/tutorial.html
- Référence de la syntaxe : https://jmespath.org/specification.html#

### Pré-traitement avant insertion en base

Pour certains cas, notamment les fichiers récupéres sous format CSV, il peut être nécessaire de “nettoyer” les fichiers avant d’insérer en base.

Le champ `load_params` est un dictionnaire clé-valeur facultatif, qui donne des paramètres de traitement qui doivent être appliqués par le Loader correspondant à `format` (ex: `JsonLoader` si le format est `json`, `CsvLoader` si le format est `csv` ), avant l’insertion en base.

Pour `format` = `csv` : 

Le `CsvLoader` peut avoir besoin des indications suivantes :

- `header` : le numéro de la première ligne du CSV. Exemple : s’il y a 2 lignes inutiles en haut du csv et que le vrai tableau de données doit commencer à la troisième ligne, alors `header` = 2 (parce que ça commence à compter à 0 )
- `skipfooter` : Même principe que `header` mais pour le bas du tableau = nombre de lignes inutiles qui doivent être ignorées tout à la fin du dataset

Exemple sur les données départementales annuelles du ministère du logement: 

```yaml
annual_dept_data:
    API: DiDo
    description: Données Annuelles Départementales de l'API DiDo
    type: FileExtractor
    endpoint: /datafiles/a0ae7112-5184-4ad7-842d-87b09fd27df1/csv
    format: csv
    extract_params:
      withColumnName: true
      withColumnDescription: true
      withColumnUnit: true
    load_params:
      header: 2 # index de la première ligne utile du csv
      skipfooter: 0 # combien de lignes inutiles à skipper en fin de document
```