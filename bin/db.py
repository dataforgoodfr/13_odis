#! /usr/bin/env python3
import os
import sys
import argparse
from dotenv import load_dotenv, dotenv_values


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.utils.database_client import DatabaseClient

load_dotenv()

DB_TARGET_NAME = os.environ['PG_DB_NAME']
SCHEMAS = ['bronze', 'silver', 'gold']

def db_connect(db_name: str) -> DatabaseClient: 
    """connect to database"""
    settings = dotenv_values()
    settings["PG_DB_NAME"] = db_name

    db_client = DatabaseClient(
        settings=settings,
        autocommit=True,
    )

    return db_client

def db_init():
    """init or reset the database"""
    db_client = db_connect("postgres")
    
    db_client.execute("SELECT pg_terminate_backend(pg_stat_activity.pid) " +
                   "FROM pg_stat_activity WHERE " + 
                   f"pg_stat_activity.datname = '{DB_TARGET_NAME}' AND pid <> pg_backend_pid()")
    
    
    db_client.execute(f"DROP DATABASE IF EXISTS {DB_TARGET_NAME}")
    print(f"Dropped existing database {DB_TARGET_NAME}")

    db_client.execute(f"CREATE DATABASE {DB_TARGET_NAME}")
    print(f"Created database {DB_TARGET_NAME}")

    db_client.close()

    db_client = db_connect("odis")
    for schema in SCHEMAS:
        db_client.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        print(f"Created schema {schema}")
    
    db_client.close()
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
