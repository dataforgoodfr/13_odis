# Élus et Nuances Politiques

Ce document récapitule la chaîne de transformation visant à lier les données des élus aux nuances politiques correspondantes au niveau communal, départemental et régional.

## 1. Sources de Données

Les modèles d'entrée sont des vues Bronze qui nécessitent des nettoyages spécifiques pour garantir l'intégrité et la joignabilité des données géographiques.

| Source | Description | Spécificité / Transformation |
| :--- | :--- | :--- |
| [presentation_elus_communes](https://public.opendatasoft.com/explore/assets/donnees-du-repertoire-national-des-elus/) | Liste complète des élus (communal, régional, départemental). |  **Nettoyage `reg_code` :** Normalisation en format texte sur 2 chiffres avec zéros à gauche (`lpad(trunc(reg_code::numeric)::text, 2, '0')`). |
| [presentation_elections_municipales](https://public.opendatasoft.com/explore/assets/election-france-municipale-2020-premier-tour/) | Candidats aux municipales. |   **Nettoyage `code_officiel_commune` :** Normalisation pour les codes à plus de 5 chiffres (ex: Outre-mer) par suppression du 3ème caractère. |
| [presentation_elections_departementales](https://www.data.gouv.fr/datasets/elections-departementales-2021-candidatures-au-1er-tour/) | Candidats aux départementales. | Source Data.gouv pour cette table car les données Opendata ne sont pas à jour |
| [presentation_elections_regionales](https://public.opendatasoft.com/explore/assets/elections-france-regionales-2021-1er-tour-par-bureau-de-vote/view/?search=eure&page=10&refine=dep_name%3AEure) | Candidats aux régionales. |  **Nettoyage et Conversion `reg_code` :** Certain reg_code sont vides, pour les outres mers(`ZA/ZB/ZC/ZD` -> `01/02/03/04`) et pour la commune 'Les Trois Lacs' (`dep_code = '27'` vers `reg_code = '28'`). |
| **`corresp_codes_nuances`** | Seed dbt. |  Table de correspondance fournissant le **libellé complet** pour chaque `code_nuance` politique. |


## 2. Transformations Intermédiaires (Silver)


Ces modèles créent des dimensions de nuance uniques en joignant les résultats électoraux au seed `corresp_codes_nuances`.

| Modèle Silver | Description | Transformations Clés |
| :--- | :--- | :--- |
| **`vw_silver_presentation_dim_nuance_politique_com`** | Nuance politique des candidats aux municipales. | Jointure des résultats municipaux sur `corresp_codes_nuances` via `code_nuance`. |
| **`vw_silver_presentation_dim_nuance_politique_dep`** | Nuance politique des binômes aux départementales. | Identique au modèle communal, appliqué aux données départementales. |
| **`vw_silver_presentation_dim_nuance_politique_reg`** | Nuance politique des têtes de liste aux régionales. |  Utilisation de la macro **`split_last_and_first_name`** pour séparer les colonnes Nom et Prénom de la tête de liste avant la jointure. |
| **`vw_silver_presentation_elus_communes`** | Liste filtrée des élus. | Filtrage de la source `presentation_elus_communes` pour ne garder que les mandats **'Départemental', 'Municipal', 'Régional'**. Renommage de `filename` en `type_de_la_fonction`. |
| **`silver_presentation_elus`** | Modèle pivot pour l'ajout de la nuance politique. |  **Jointures conditionnelles :** Ajout de la nuance politique (`libelle_nuance`) uniquement pour les fonctions de `Maire, Président CD, Président CR` en utilisant le code géographique correspondant. |



## 3. Modèle de Présentation (Gold)


| Modèle Gold | Description | Transformations Clés |
| :--- | :--- | :--- |
| **`gold_presentation_elus`** | Modèle final. | **1. Création de la Clé Géographique (`codgeo`) :** Normalise la clé géographique selon le niveau de fonction (Maire = `com_code`, Président CD = `dep_code`, Président CR = `regXX`). **2. Nettoyage de la Nuance :** Si la nuance est `NULL`, la valeur est remplacée par la chaîne **'Non connu'**. **3. Renommage :** Simplification des noms de colonnes (`prenom`, `nom`, `fonction`, `libelle`). |