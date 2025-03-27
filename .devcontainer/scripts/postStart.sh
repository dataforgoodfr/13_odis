#!/bin/sh
echo "----- postStart.sh -----"
echo "Current directory: $(pwd)"
echo "Current user: $(whoami)"

# Install pre-commit hooks
#pre-commit install
#pip install -r $(pwd)/requirements.txt

echo "----- Install poetry -----"
cp .env.dist .env
poetry lock
poetry install

#echo "----- Install ipykernel -----"
#poetry add ipykernel
#poetry add pandas psycopg2

echo "----- Install ipykernel -----"
poetry run python -m ipykernel install --user --name=d4g --display-name "Python (d4g)"

echo "----- Install dbt-core -----"
pip install dbt-core
pip install dbt-postgres
pip install boto3


# Vérifier l’installation
dbt --version

echo "----- EOF postStart.sh -----"
