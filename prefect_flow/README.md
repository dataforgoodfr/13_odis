# 📘 README – Exécution du pipeline Prefect 3.6

Ce projet utilise **Prefect 3.6**, qui sépare nettement :

* l’**API Orion** (serveur)
* le **Déploiement / Serveur de Flow**
* les **Workers** qui exécutent les tâches

C’est ce découpage qui explique pourquoi **plusieurs terminaux** sont nécessaires.

---

# 🚀 1. Prérequis

Créer l’environnement virtuel :

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cd prefect_flow; docker compose up -d
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
prefect config set PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://prefect:prefect@localhost:5432/prefect"

```
Vous pourrez ensuite vérifier qu'aucun dossier storage/ est créé dans ~/.prefect/

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
prefect server start
```

---

### ⚙️ TERMINAL 2 — **Flow Server (serve)**

Quand tu lances :

```bash
python -m prefect_flow.flow
```

ton code appelle :

```python
full_pipeline.serve()
```

Cela crée :

* un **deployment** (“full-pipeline”)
* un **work pool** (default)
* un service qui dit au serveur : “j’existe, voici ma définition de flow”.

Ce terminal doit rester **ouvert** : c’est lui qui déclare ton flow au serveur.

---

### 🏭 TERMINAL 3 — **Worker**

Le worker exécute les runs.
Il se connecte au work pool et récupère les tâches à exécuter.

Il doit savoir où se trouve le serveur → d’où la variable :

```
PREFECT_API_URL=http://127.0.0.1:4200/api
```

Il doit tourner **en continu** comme un job supervisor.

```bash
export PREFECT_API_URL=http://127.0.0.1:4200/api ; prefect worker start -q default
```

---

### 🕹️ TERMINAL 4 — **Déclenchement d’un run**

Ce terminal est optionnel : il sert uniquement à lancer un run manuel.

```bash
prefect deployment run "full-pipeline/full_pipeline"
```


# DEBUG

if __name__ == "__main__":
    odis_pipeline(
        config_path="datasources.yaml",
        max_concurrency=4,
    )