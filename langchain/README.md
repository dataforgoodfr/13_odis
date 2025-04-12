# LangChain Demo avec Ollama et le modèle Mistral

Ce projet démontre l'utilisation de LangChain pour interagir avec une base de données SQL en utilisant des modèles de langage. 
Si vous ne disposez pas d'un compte OpenAI API, vous pouvez tester ce code en local avec **Ollama** et le modèle **Mistral**.

## Instructions pour utiliser Ollama et le modèle Mistral

### Étape 1 : Installation d'Ollama
Ollama est installé automatiquement lors de la création du devcontainer grâce au script d'installation dans le `Dockerfile` :

```bash
RUN curl -sSfL https://ollama.com/download.sh | sh
```

### Étape 2 : Lancer le serveur Ollama
Une fois le devcontainer créé, lancez le serveur Ollama depuis un terminal et gardez cette fenêtre ouverte :

```
ollama serve
```

### Étape 3 : Installer le modèle Mistral
Dans une nouvelle fenêtre de terminal, installez le modèle Mistral :

```
ollama pull mistral
```

### Étape 4 : Lancer le script

Si vous souhaitez utiliser le modèle Mistral au lieu d'OpenAI, remplacez la ligne suivante dans le code :

```
llm = ChatOpenAI(temperature=temperature, model=model)
```

par :

```
llm = ChatOllama(model=model, temperature=temperature)
```

Exécutez le script Python pour interagir avec la base de données :

```
python langchain/demo.py
```
