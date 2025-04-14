# manage here test fixtures specific to the module

from __future__ import annotations

import time

import docker
import psycopg2
import pytest

from common.utils.interfaces.data_handler import PageLog, StorageInfo

PG_IMAGE_NAME = "postgres:17"
PG_CONTAINER_NAME = "test_postgres"

PG_USER = "odis"
PG_HOST = "localhost"
PG_PORT = "5433"
PG_PASSWORD = "odis"
PG_DATABASE = "odis_test"

SCHEMAS = ["bronze", "silver", "gold"]

# see https://stackoverflow.com/questions/46733332/how-to-monkeypatch-the-environment-using-pytest-in-conftest-py
mp = pytest.MonkeyPatch()

# postgresql
mp.setenv("DB_USER", PG_USER)
mp.setenv("DB_PASSWORD", PG_PASSWORD)
mp.setenv("DB_HOST", PG_HOST)
mp.setenv("DB_PORT", PG_PORT)
mp.setenv("DB_DATABASE", PG_DATABASE)


def pytest_addoption(parser):
    parser.addoption(
        "--standalone",
        default="false",
        help=("pass --standalone=true to start tests without launching postgre"),
        choices=("true", "false"),
    )


@pytest.fixture(scope="session", autouse=True)
def launch_postgre_container(request):

    with_pg = str(request.config.getoption("--standalone")).strip() in [
        "",
        "false",
    ]

    if with_pg:
        start_time = time.time()

        docker_client = docker.from_env()
        docker_client.containers.prune()

        print("--- Starting PostgreSQL Container ---")

        container = docker_client.containers.run(
            PG_IMAGE_NAME,
            detach=True,
            name=PG_CONTAINER_NAME,
            ports={"5432": PG_PORT},
            environment={"POSTGRES_USER": PG_USER, "POSTGRES_PASSWORD": PG_PASSWORD},
        )

        wait_for_postgre = True
        print("--- Launching PostgreSQL Container ---")
        while wait_for_postgre:
            logs = container.logs()
            time.sleep(0.5)
            elapsed = time.time() - start_time
            print(str(logs))
            if "ready to accept connections" in str(logs):
                wait_for_postgre = False
                print(f"\r\nLaunched PostgreSQL in {round(elapsed, 2)} seconds")
                print("--- PostgreSQL Container started ---")
                print("--- PostgreSQL Container: Apply migration ---")
                # subprocess.run(["alembic", "-n", "engine2", "upgrade", "head"], capture_output=True)
                print("--- PostgreSQL Container: Migrations applied ---")

    yield

    if with_pg:
        try:
            ctn = docker_client.containers.get(PG_CONTAINER_NAME)
            ctn.remove(force=True)
        except docker.errors.NotFound:
            pass


@pytest.fixture(scope="function")
def pg_settings():
    """return the settings for the database connection"""
    yield {
        "PG_DB_USER": PG_USER,
        "PG_DB_PWD": PG_PASSWORD,
        "PG_DB_HOST": PG_HOST,
        "PG_DB_PORT": PG_PORT,
        "PG_DB_NAME": PG_DATABASE,
    }


@pytest.fixture(scope="function")
def pg_con(launch_postgre_container):
    """provide a connection to the database"""
    # This fixture is used in each test function
    # It provides a connection to the PostgreSQL database
    print("--- Creating PostgreSQL Connection ---")
    connection = psycopg2.connect(
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
    )
    connection.autocommit = True

    yield connection

    print("--- Closing PostgreSQL Connection ---")
    connection.close()
    print("--- PostgreSQL Connection closed ---")


@pytest.fixture(scope="session", autouse=True)
def init_db(request, launch_postgre_container):
    """
    Fixture to initialize the database before running tests.
    This fixture is automatically used in all test files.
    """
    # Here you can add code to initialize the database
    # For example, create tables, insert test data, etc.
    # This will run once per session
    print("--- Initializing Database ---")

    co = psycopg2.connect(
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
    )
    co.autocommit = True

    cursor = co.cursor()

    cursor.execute(f"CREATE DATABASE {PG_DATABASE}")

    co.close()

    co = psycopg2.connect(
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
    )

    cursor = co.cursor()

    for schema in SCHEMAS:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        co.commit()
        print(f"Created schema {schema}")

    cursor.close()
    co.close()
    print("Database initialization completed successfully")

    yield
    print("--- Cleaning up Database ---")
    # Here you can add code to clean up the database after tests


@pytest.fixture(scope="function")
def stub_page_log():

    page = 1
    storage_info = StorageInfo(
        location="data/imports",
        format="json",
        file_name="logement.logements_maison_et_residences_principales_1.json",
        encoding="utf-8",
    )
    is_last = False
    success = True

    yield PageLog(
        page=page, storage_info=storage_info, is_last=is_last, success=success
    )
