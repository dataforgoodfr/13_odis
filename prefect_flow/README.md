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



Oui — ce log donne **l’information clé** qui manquait :
le problème n’est pas seulement la session aiohttp non fermée, mais aussi que tu passes **des objets non‐awaitables** dans ton `asyncio.gather()`.

Regarde :

```
TypeError: An asyncio.Future, a coroutine or an awaitable is required
```

et juste au-dessus :

```
artifacts = await asyncio.gather(*extract_tasks, return_exceptions=True)
                ^^^^^^^^^^^^^^^
```

Donc **`extract_tasks` ne contient pas des coroutines**… mais autre chose.

Et en regardant ton code, on comprend pourquoi 👇

---

# 💥 Cause exacte

Tu construis ta liste ainsi :

```python
extract_tasks = [
    prefect_extract.with_options(name=f"Extract {ds.name}").submit(config, ds, max_concurrency)
    for ds in config.get_models().values()
]
```

Et **`.submit()` retourne un objet Prefect TaskRunResult**, pas une future asyncio.
Donc :

* `submit()` = exécution **async orchestrée par Prefect**, mais **pas une coroutine**
* `await asyncio.gather(...)` = attend **des coroutines Python**, pas des tâches Prefect

Donc mélanger les deux = ❌ crash.

---

# 🧠 Théorie Prefect (clair et simple)

Deux modèles existent :

## **Modèle 1 : Async Python**

* Tu appelles une `@task async`
* Tu récupères un `coroutine`
* Tu `await` avec `gather`

Exemple :

```python
tasks = [prefect_extract(...), prefect_extract(...)]
results = await asyncio.gather(*tasks)
```

## **Modèle 2 : Orchestration Prefect**

* Tu appelles `.submit()`
* Tu récupères un `PrefectFuture`
* Tu `.result()` (sync) ou `.wait()` (async)

Exemple :

```python
tasks = [prefect_extract.submit(...), prefect_extract.submit(...)]
results = [task.result() for task in tasks]
```

👉 **NE JAMAIS mélanger les deux modèles**.

Et c’est exactement ce que ton code fait aujourd’hui.

---

# 🟢 Comment corriger

Tu dois **choisir un modèle** :

---

## ✔️ Option A : Flow 100% async Python (recommandé pour API / aiohttp)

### 1) Task async

```python
@task
async def prefect_extract(config, ds, max_concurrency):
    return await run_extraction(config, [ds], max_concurrency)
```

### 2) Flow async

```python
@flow
async def full_pipeline(...):
    config = load_config(...)

    tasks = [
        prefect_extract.with_options(name=f"Extract {ds.name}")(
            config, ds, max_concurrency
        )
        for ds in config.get_models().values()
    ]

    artifacts = await asyncio.gather(*tasks, return_exceptions=True)
```

👉 Pas de `.submit()`.

* Prefect détecte que ce sont des coroutines et les exécute dans un seul processus
* parfait si `run_extraction` fait de l’I/O concurrent

---

## ✔️ Option B : Flow orchestré Prefect (pour scalabilité / workers)

Si tu veux lancer **chaque extraction sur un worker séparé**, tu fais :

```python
tasks = [
    prefect_extract.with_options(name=f"Extract {ds.name}").submit(
        config, ds, max_concurrency
    )
    for ds in config.get_models().values()
]
```

Puis :

```python
artifacts = [t.result() for t in tasks]
```

👉 Ici **pas de `await asyncio.gather()`**.

C’est Prefect qui gère le scheduling, l’async, les retries, etc.
Ton flow reste **synchrone**, mais distribué.

---

# ❗ À propos de l’erreur aiohttp « Unclosed client session »

Elle est probablement causée par :

* des sessions ouvertes dans `run_extraction`
* un mauvais pattern async
* l’event loop forcée par Prefect

Quand tu corrigeras **l’un des deux modèles**, l’erreur a de grandes chances de disparaître.

Mais vérifie quand même :

```python
async with aiohttp.ClientSession() as session:
    ...
```

(❌ pas de `session = ClientSession()` sans close)

---

# 🎯 Recommandation

Vu ton code, je pense que tu veux :

* plusieurs extracteurs en parallèle,
* mais dans **le même process**
* avec async+await réel
* sans worker Dist Prefect

Donc **Option A** est ton meilleur choix.

---

# ✨ Résumé ultra court

L’erreur vient de :

```
.submit() + asyncio.gather() = incompatible
```

Corrige en :

* soit `await gather(tasks)` **sans submit**
* soit `submit()` **sans gather**

Et ta stack trace disparaît 💪

---

Si tu veux, je peux te proposer une **version corrigée complète de ton flow** + correction dans `run_extraction` pour fermer les sessions aiohttp proprement.
