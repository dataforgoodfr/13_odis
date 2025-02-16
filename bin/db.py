#! /usr/bin/env python3
import os
import argparse
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, connection
from dotenv import load_dotenv

load_dotenv()

DB_BASE_CONFIG = {
    'dbname': 'postgres',
    'user': os.environ['PG_DB_USER'],
    'password': os.environ['PG_DB_PWD'],
    'host': 'localhost',
    'port': '5432'
}

DB_TARGET_NAME = os.environ['PG_DB_NAME']
SCHEMAS = ['bronze', 'silver', 'gold']

def db_connect() -> connection: 
    """connect to database"""
    try: 
        co = psycopg2.connect(**DB_BASE_CONFIG)
        co.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return co
    except psycopg2.Error as e:
        print(f"Failed to connect to database: {e}")

def db_init():
    """init or reset the database"""
    co = db_connect()
    cursor = co.cursor()

    # Check if database is already setup
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_TARGET_NAME}'")
    
    if cursor.fetchone():
        response = input(f"Database '{DB_TARGET_NAME}' already exists. Do you want to reinitialize it? (y/N): ")
        if response.lower() != 'y':
            print("Database initialization cancelled.")
            return
    
    cursor.execute(f"DROP DATABASE IF EXISTS {DB_TARGET_NAME}")
    print(f"Dropped existing database {DB_TARGET_NAME}")

    cursor.execute(f"CREATE DATABASE {DB_TARGET_NAME}")
    print(f"Created database {DB_TARGET_NAME}")

    for schema in SCHEMAS:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        print(f"Created schema {schema}")
    
    cursor.close()
    co.close()
    print("Database initialization completed successfully")

def main():
    parser = argparse.ArgumentParser(description='Database management script')
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    subparsers.add_parser('init', help='Init or reset the database.')
    args = parser.parse_args()

    if args.command == 'init':
        db_init()
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
