# Extraction de Données

Les sources de données utiles au projet sont diverses avec chacune leurs spécificités mais peuvent présenter certaines caractéristiques communes : beaucoup sont des sources OpenData exposant des API suivant le standard REST, certaines suivent le standard spécifique OpendataSoft, la plupart permettent de récupérer des données sous format JSON et CSV, etc…

Les spécificités de chacune se situent souvent dans la structure de leurs champs, le nom des paramètres à passer, les URL à trouver et appeler, les contraintes à respecter :

- taille de fichiers, pagination ( = découpage des résultats en plusieurs morceaux )
- ne pas dépasser une certaine fréquence d’appels ( throttling )
- certains paramètres à passer dans les requêtes, des headers à préciser…
- dans certains cas (plutôt rares), présenter une authentification ( = un mot de passe ou équivalent)

Pour rendre l’extraction de ces diverses sources facilement extensible (ie permettre de rapidement rajouter des nouvelles sources d’extraction sans trop de code) tout en restant assez flexible pour s’adapter aux spécificités de chacune, nous avons adopté une approche basée sur des configurations déclaratives, et des objets Extracteurs.

## Extractors

Les Extractors sont des classes Python, définies dans le fichier `common/utils/source_extractors.py`. Chaque Extractor déclare une méthode `extract` servant à requêter des API, et récupérer ce qui sort pour le stocker dans un format adéquat (JSON ou CSV par exemple), ou bien le passer à une autre fonction Python qui continuerait la chaîne d’ELT.

Un Extractor peut être assez générique, pour être réutilisé dans divers cas : `FileExtractor` par exemple, qui récupère un fichier entier depuis n’importe quelle API http sans authentification ni pagination. Ou au contraire très spécifique et adapté à un cas particulier : récupérer une API avec des contraintes très particulières d’authentification, de format, de pagination par exemple.

Les Extractors sont dérivés d’une classe abstraite `SourceExtractor` qui définit certaines propriétés et méthodes communes, qui sont donc héritées et utilisable par tout Extracteur. En particulier, la fonction `set_query_parameters` qui permet d’interpréter la configuration déclarative et prépare les différents paramètres pour envoyer une requête à une API.

## Configuration déclarative des sources

Le fichier `datasources.yaml` répertorie toutes nos sources de données et est organisé autour de trois grandes notions :

- Les API sur lesquelles on va chercher la donnée ( APIs INSEE, API du ministère du logement, etc…)
- Les “Domaines” de donnée qui regroupent les jeux de données en thématiques : géographie, logement, emploi etc
- Au sein de chaque Domaine, des “Sources” qui représentent les informations sur les jeux de données précis à récupérer

Les définitions sont données dans un fichier yaml pour être facilement lisibles et extensibles. Le fichier est organisé en deux grands blocs :

### Définition des API

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

Optionnellement, un bloc API peut déclarer toute information utile pour prendre en compte les contraintes de l’API : headers à passer, paramètres obligatoires, paramètres de pagination, etc…

### Définition des modèles Sources

Les jeux de données Source sont organisées par domaine, dans le bloc `domains` :

```yaml
domains:

  geographical_references:

    regions:
      API: INSEE.Metadonnees
      description: Référentiel géographique INSEE - niveau régional
      type: JsonExtractor
      urlpath: /geo/regions

    departements:
      API: INSEE.Metadonnees
      description: Référentiel géographique INSEE - niveau départemental
      type: JsonExtractor
      urlpath: /geo/departements
```

Dans l’exemple ci-dessus, est déclaré le domaine “geographical_references”, qui contient les modèles source pour les jeux de donnée “régions” et “départements” du référentiel géographique de l’INSEE.

Un bloc “source” définit obligatoirement les champs suivants :

- `API` : quelle API est à la source de ce dataset
- `description` : description claire et concise pour aider à la compréhension
- `type` : quel type d’Extracteur doit être utilisé pour récupérer ce dataset
- `endpoint` : c'est un endpoint (point de terminaison) permettant d'accèder au jeu de données. C'est ce qui doit compléter l'URL de base de l'API pour trouver un jeu de données
  
Dans l’exemple donné, pour récupérer le dataset “regions”, un Extracteur de classe “JsonExtractor” sera donc instancié, pour requêter l’API INSEE.Metadonnees sur l’URL complète suivante :

`https://api.insee.fr/metadonnees/v1/geo/regions`

## Comment ajouter des nouvelles sources

Pour ajouter des nouveaux dataset sources à extraire, il faut donc en prérequis, avoir exploré et identifié quelles API et quelles jeux de données sont intéressants, et avoir compris comment lesdites API fonctionnent.

Une fois que vous savez ce que vous voulez récupérer, il faut ajouter une ou deux choses au code :

1. Créer une nouvelle branche “feat” à partir de “main”, et associée à une Task dans le board Github
2. Ajouter les déclarations d’API, domaine et sources dans `datasources.yaml` , selon le format décrit plus haut
3. Si aucun des Extractors existants dans `common/utils/source_extractors.py` ne convient aux besoins et contraintes de l’API que vous ciblez, vous pouvez soit modifier un Extractor existant, soit en créer un nouveau adapté à vos besoins, en respectant les deux contraintes suivantes :
    1. Tout Extractor doit hériter de `SourceExtractor`
    2. Tout Extractor doit définir une méthode `extract`
4. La valeur du champ `type` pour vos définitions de sources dans la config doit correspondre exactement au nom de votre classe d'extracteur (par exemple FileExtractor)
5. Tester en local, et une fois que ça marche pour vous, soumettre une PR 

Good luck 🙂
