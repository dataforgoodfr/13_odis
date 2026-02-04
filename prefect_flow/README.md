# 📘 README – Exécution du pipeline Prefect 3.6

Ce projet utilise **Prefect 3.6**, qui sépare nettement :

* l’**API Orion** (serveur)
* le **Déploiement / Serveur de Flow**
* les **Workers** qui exécutent les tâches

C’est ce découpage qui explique pourquoi **plusieurs terminaux** sont nécessaires.

---

# 🚀 1. Prérequis

Installer si besoin les dépendances:

```bash
poetry install
```

```bash
docker compose up -d
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
prefect config set PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://prefect:prefect@localhost:5432/prefect"

```
Vous pourrez ensuite vérifier qu'aucun dossier storage/ est créé dans ~/.prefect/ (donc Prefect utilise bien Postgresql et non pas sqllite)
Il est également important d'initialiser la base de donnée Postgresql comme indiqué dans le README à la racine du repo.

---

# 🧠 Architecture Prefect 3.6 — Pourquoi 3 terminaux ?

Prefect fonctionne selon trois rôles complémentaires :

### 🖥️ TERMINAL 1 — **Serveur Prefect (API + UI)**

C’est le "cerveau".
Il stocke :

* les flows
* les runs
* l’historique
* les logs
* les orchestrations

Sans le serveur → impossible de déclencher un flow.

```bash
poetry run prefect server start
```

---

### 🏭 TERMINAL 2 — **Worker**

Le worker exécute les runs.
Il se connecte au work pool et récupère les tâches à exécuter.

Il doit savoir où se trouve le serveur → d’où la variable :

```
PREFECT_API_URL=http://127.0.0.1:4200/api
```

Il doit tourner **en continu** comme un job supervisor.

```bash
export PREFECT_API_URL=http://127.0.0.1:4200/api ; poetry run prefect worker start -p default
```

---

### 🕹️ TERMINAL 3 — **Déclenchement d’un run**

Il sert à enregistrer un "deployment" dans prefect et à lancer un run manuel. Il faut d'abord lancer cette commande : 

```bash
poetry run python prefect_flow/deploy.py 
```

Ensuite ton flow sera "inscrit" dans l'interface. Tu peux déclencher un run manuellement, ou via le CLI prefect :


```bash
poetry run prefect deployment run "full-pipeline/full-pipeline"
```
En cas d'erreur:

```bash
ValueError: ZoneInfo keys may not be absolute paths, got: /UTC
```

Ajouter ceci, par exemple, aux variables d'environnement:

```bash
export TZ=Europe/Paris
```

# DEBUG

if __name__ == "__main__":
    odis_pipeline(
        config_path="datasources.yaml",
        max_concurrency=4,
    )

# Concurrency limit

prefect concurrency-limit create api.insee.melodi 1
prefect concurrency-limit create api.insee.metadonnees 1
prefect concurrency-limit create api.insee.statistiques 1
prefect concurrency-limit create api.data.gouv.fr 1
prefect concurrency-limit create api.opendatasoft 2
prefect concurrency-limit create api.geo.api.gouv.fr 2
