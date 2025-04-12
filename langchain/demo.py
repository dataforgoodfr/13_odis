#!/usr/bin/env python3
"""
Demo script showing SQL database interaction with LangChain and Ollama.
"""
import os
import logging
from typing import Optional
from dotenv import load_dotenv
from langchain_community.utilities import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain_openai import ChatOpenAI
#from langchain_community.chat_models import ChatOllama
from langchain_ollama import ChatOllama

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


def create_sql_chain(db_uri: str, temperature: float = 0, model: str = "mistral") -> SQLDatabaseChain:
    """
    Create a SQL database chain with the specified LLM and database.
    
    Args:
        db_uri: Database connection URI
        temperature: Temperature setting for the LLM (0 = deterministic)
        model: Model name to use for the LLM
        
    Returns:
        SQLDatabaseChain instance
    """
    # Connect to the database et lui dire de regarder dabs le schéma silver (changer en gold quand dispo)
    db = SQLDatabase.from_uri(db_uri, schema="silver")
    
    # Initialize the LLM
    #llm = ChatOpenAI(temperature=temperature, model=model) # Pour utiliser OpenAI 
    llm = ChatOllama(model=model, temperature=temperature)  # Pour utiliser Ollama model en local si disponible
    
    # Create the chain
    return SQLDatabaseChain.from_llm(llm=llm, db=db, verbose=True)


def query_database(chain: SQLDatabaseChain, query: str) -> Optional[str]:
    """
    Run a natural language query against the database using the LLM chain.
    
    Args:
        chain: The SQLDatabaseChain to use
        query: Natural language query to process
        
    Returns:
        Response from the LLM or None if error occurred
    """
    try:
        logger.info(f"Processing query: {query}")
        response = chain.invoke(query)
        return response
    except Exception as e:
        logger.error(f"Error processing query: {e}")
        return None


def main():
    """Main function to demonstrate database querying with LangChain.
    
    OpenAI n'est pas opensource. Si on n'a pas de compte OpenAI API, on peut tester ce code en local en utilisant Ollama.
    Si on veut utiliser le modèle mistral, il faut remplacer la ligne :
        llm = ChatOpenAI(temperature=temperature, model=model)
    par :
        llm = ChatOllama(model=model, temperature=temperature)


    Pour utiliser Ollama, il faut installer ollama et le modèle mistral :
    1. Ollama a été installé dès la création du devcontainer, dans le Dockerfile grâce au script d'installation :
        curl -sSfL https://ollama.com/download.sh | sh
    2. Une fois le devcontainer créé, lLancer le serveur ollama depuis un terminal, et garder cette fenêtre de terminal ouverte :
        ollama serve
    3. Sans fermer le terminal, ouvrir une nouvelle fenêtre du terminal et installer le modèle mistral :    
        ollama pull mistral
    4. Lancer le script :
        python demo.py
  
    
    """
    # Database connection string - consider moving to environment variables
    db_uri = os.getenv("DATABASE_URI", "postgresql+psycopg2://odis:odis@localhost:5432/odis")
    
    # Create the SQL chain
    db_chain = create_sql_chain(db_uri)

    # 🔍 Affiche les tables disponibles (debug)
    print("Tables disponibles dans la base de données :")
    print(db_chain.database.get_usable_table_names())

    # Example query
    query = "Combien de communes en france ?"
    logger.info(f"Executing query: '{query}'")
    
    # Run the query
    response = query_database(db_chain, query)
    
    # Display results
    if response:
        print("\nResponse:")
        print(response)


if __name__ == "__main__":
    main()