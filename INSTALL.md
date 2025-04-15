# Guide d'installation

## Installer Docker

Suivre les instructions sur le [site officiel](https://www.docker.com/get-started/)

## Installer Poetry

Poetry est l'outil que nous utilisons pour gérer les dépendances et l'environnement Python du projet.
Pour utiliser Poetry, tu as deux options :

### Utilisation d'un devcontainer (recommandée)

Si tu préfères ne rien installer localement et travailler dans un environnement isolé, tu peux utiliser Devcontainer. C'est un environnement de développement, principalement utilisé avec des outils comme VS Code. Il est configuré dans le fichier devcontainer/devcontainer.json pour spécifier notre environnement python, y compris l'installation de Poetry et d'autres dépendances.

Pour installer l'extension Dev Containers dans VS Code :

```bash
code --install-extension ms-vscode-remote.remote-containers
```

Une fois l'extension installée, tu pourras ouvrir le projet dans un Devcontainer ignorer les autres installations et passer directement à la configuration de l'environnement.

### Installation locale de Poetry

Si tu préfères installer Poetry directement sur ta machine (et non dans un Devcontainer), tu peux choisir parmi plusieurs[méthodes d'installation](https://python-poetry.org/docs/#installation) décrites dans la documentation officielle de Poetry, notamment :

- avec pipx
- avec l'installateur officiel

Chaque méthode a ses avantages et inconvénients. Par exemple, la méthode pipx nécessite d'installer pipx au préable, l'installateur officiel utilise curl pour télécharger un script qui doit ensuite être exécuté et comporte des instructions spécifiques pour la completion des commandes poetry selon le shell utilisé (bash, zsh, etc...).

L'avantage de pipx est que l'installation de pipx est documentée pour linux, windows et macos. D'autre part, les outils installées avec pipx bénéficient d'un environment d'exécution isolé, ce qui est permet de fiabiliser leur fonctionnement. Finalement, l'installation de poetry, voire d'autres outils est relativement simple avec pipx.

Cependant, libre à toi d'utiliser la méthode qui te convient le mieux ! Quelque soit la méthode choisie, il est important de ne pas installer poetry dans l'environnement virtuel qui sera créé un peu plus tard dans ce README pour les dépendances de la base de code de ce repo git.

#### Installation de Poetry avec pipx

Suivre les instructions pour [installer pipx](https://pipx.pypa.io/stable/#install-pipx) selon ta plateforme (linux, windows, etc...)

Par exemple pour Ubuntu 23.04+:

    sudo apt update
    sudo apt install pipx
    pipx ensurepath

[Installer Poetry avec pipx](https://python-poetry.org/docs/#installing-with-pipx):

    pipx install poetry

#### Installation de Poetry avec l'installateur officiel

L'installation avec l'installateur officiel nécessitant quelques étapes supplémentaires,
se référer à la [documentation officielle](https://python-poetry.org/docs/#installing-with-the-official-installer).

#### Utiliser un venv python

    python3 -m venv .venv

    source .venv/bin/activate

#### Utiliser Poetry

Installer les dépendances:

    poetry install

Ajouter une dépendance:

    poetry add pandas

Mettre à jour les dépendances:

    poetry update

#### Lancer les precommit-hook localement

[Installer les precommit](https://pre-commit.com/)

    pre-commit run --all-files

#### Utiliser Tox pour tester votre code

    tox -vv
