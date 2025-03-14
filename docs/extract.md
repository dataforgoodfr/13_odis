# Extracteurs de données

Created: March 12, 2025 10:16 AM
type: documentation

Les sources de données utiles au projet sont diverses avec chacune leurs spécificités mais peuvent présenter certaines caractéristiques communes : beaucoup sont des sources OpenData exposant des API suivant le standard REST, certaines suivent le standard spécifique OpendataSoft, la plupart permettent de récupérer des données sous format JSON et CSV, etc…

Les spécificités de chacune se situent souvent dans la structure de leurs champs, le nom des paramètres à passer, les URL à trouver et appeler, les contraintes à respecter :

- taille de fichiers, pagination ( = découpage des résultats en plusieurs morceaux )
- ne pas dépasser une certaine fréquence d’appels ( throttling )
- certains paramètres à passer dans les requêtes, des headers à préciser…
- dans certains cas (plutôt rares), présenter une authentification ( = un mot de passe ou équivalent)

Pour rendre l’extraction de ces diverses sources facilement extensible (ie permettre de rapidement rajouter des nouvelles sources d’extraction sans trop de code) tout ne restant assez flexible pour s’adapter aux spécificités de chacune, nous avons adopté une approche basée sur des configurations déclaratives, et des objets Extracteurs.

La configuration déclarative (datasources.yaml) est expliquée ici : [Config déclarative des sources](./docs/configurations.md)

## Extractors

Les Extractors sont des classes Python, définies dans le fichier `common/utils/source_extractors.py`. Chaque Extractor déclare une méthode `extract` servant à requêter des API, et récupérer ce qui sort pour le stocker dans un format adéquat (JSON ou CSV par exemple), ou bien le passer à une autre fonction Python qui continuerait la chaîne d’ELT.

Un Extractor peut être assez générique, pour être réutilisé dans divers cas : `FileExtractor` par exemple, qui récupère un fichier entier depuis n’importe quelle API http sans authentification ni pagination. Ou au contraire très spécifique et adapté à un cas particulier : récupérer une API avec des contraintes très particulières d’authentification, de format, de pagination par exemple.

Les Extractors sont dérivés d’une classe abstraite `SourceExtractor` qui définit certaines propriétés et méthodes communes, qui sont donc héritées et utilisable par tout Extracteur. En particulier, la fonction `set_query_parameters` qui permet d’interpréter la configuration déclarative et prépare les différents paramètres pour envoyer une requête à une API.

## Gestion de la pagination

La plupart des API imposent des contraintes de pagination et de throttling (ou rate limit) : on est alors obligé de récupérer des data en plusieurs pages, avec un appel API par page, et on ne peut pas envoyer plus de ‘n’ requêtes par minute.

Pour gérer cela, tous les Extractors se comportent comme s’ils allaient paginer, en se basant sur des indications dans la config `datasources.yaml` . Ces indications sont définies dans le bloc `response_map` :

```yaml
logements_total:
    API: INSEE.Melodi
    description: nombre de logement
    type: MelodiExtractor
    endpoint: /data/DS_RP_LOGEMENT_PRINC
    format: json
    response_map: # section qui indique comment interpreter des champs de la réponse
      data: observations
      next: paging.next # où trouver le champ qui donne la ref de la prochaine page
      is_last: paging.isLast # où trouver le champ qui dit si c'est la dernière page
```

Si on définit une valeur pour les champs `next` et `is_last` , ils sont récupérés dans la réponse de l’API pour permettre de continuer la pagination.

<aside>
💡

Si on ne définit pas les champs `next` ou `is_last` , l’extractor marche, il va juste se comporter comme s’il n’y avait qu’une seule page.

</aside>

## Gestion du throttling

Le paramètre de throttling est défini au niveau de la définition d’un bloc `API` et est exprimé en nombre de requêtes par minute :

```yaml
INSEE.Melodi:
    name: MELODI
    description: INSEE - API de données locales
    base_url: https://api.insee.fr/melodi
    apidoc: https://portail-api.insee.fr/catalog/api/a890b735-159c-4c91-90b7-35159c7c9126/doc?page=ee625968-272a-4637-a259-68272aa63766
    throttle: 30 # requetes / minutes
    default_headers:
      accept: application/json
```

## Comment ajouter un nouvel Extracteur

Si aucun des Extractors existants dans `common/utils/source_extractors.py` ne convient aux besoins et contraintes de l’API que vous ciblez, vous pouvez soit modifier un Extractor existant, soit en créer un nouveau adapté à vos besoins.

Pour créer un nouvel Extractor, il faut respecter les contraintes suivantes :

1. Tout Extractor doit hériter de `SourceExtractor`
2. Tout Extractor doit définir une méthode `extract` , qui retourne un **générateur** python, c’est à dire que la fonction ne renvoie pas un “return”, mais un “yield”
3. Le générateur de la méthode `extract` doit yield la signature suivante : 
    1. `payload` : le contenu de la réponse de l’API (json, csv ou autre)
    2. `page_number` (int) : le numéro de la page qu’on vient de récupérer
    3. `is_last` (bool) : est-ce que la page récupérée était la dernière
    4. `filepath` (str) : le path vers le fichier dumpé en local, s’il existe

Good luck 🙂