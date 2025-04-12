#!/bin/sh
echo "----- postStart.sh -----"
echo "Current directory: $(pwd)"
echo "Current user: $(whoami)"

# Install PostgreSQL client and build dependencies for psycopg2
echo "----- Installing PostgreSQL dependencies for psycopg2 -----"
sudo apt-get update && sudo apt-get install -y libpq-dev gcc

echo "----- Setup .env file -----"
if [ ! -f .env ]; then
    echo ".env file does not exist, copying from .env.dist"
    cp .env.dist .env
else
    echo ".env file already exists, skipping copy"
fi

# Install poetry dependencies
echo "----- Install poetry dependencies -----"
poetry lock
poetry install

# Install pip dependencies like papermill
echo "----- Install papermill -----"
pip install papermill

# Install Jupyter kernel
echo "----- Install ipykernel -----"
poetry run python -m ipykernel install --user --name=d4g --display-name "Python (d4g)"

# Install pip dependencies like papermill
echo "----- Install dbt -----"
pip install dbt-core 
echo "----- Adapter pour PostgreSQL  -----"
pip install dbt-postgres
echo "----- Vérifier l'installation -----" 
dbt --version
echo "-----  Installation des dépendences -----" 
dbt deps

#echo "----- Install dependencies -----"
#poetry add pandas psycopg2 ipykernel

echo "----- EOF postStart.sh -----"

