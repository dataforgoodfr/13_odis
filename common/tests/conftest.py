from __future__ import annotations

import time
import docker
import psycopg2
import pytest

from common.utils.interfaces.data_handler import PageLog, StorageInfo

PG_IMAGE_NAME = "postgres:17"
PG_CONTAINER_NAME = "test_postgres"

PG_USER = "odis"
PG_PASSWORD = "odis"
PG_HOST = "localhost"
PG_PORT = 5432
PG_DATABASE = "odis_test"

SCHEMAS = ["bronze", "silver", "gold"]


def wait_for_postgres(
    host,
    port,
    user,
    password,
    dbname="postgres",
    timeout=30,
):
    start = time.time()
    last_error = None

    while time.time() - start < timeout:
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=dbname,
            )
            conn.close()
            return
        except psycopg2.OperationalError as e:
            last_error = e
            time.sleep(0.5)

    raise TimeoutError(
        f"PostgreSQL not ready after {timeout}s. Last error: {last_error}"
    )


@pytest.fixture(scope="session")
def postgres_container():
    docker_client = docker.from_env()

    try:
        docker_client.containers.get(PG_CONTAINER_NAME).remove(force=True)
    except docker.errors.NotFound:
        pass

    container = docker_client.containers.run(
        PG_IMAGE_NAME,
        detach=True,
        name=PG_CONTAINER_NAME,
        ports={"5432/tcp": PG_PORT},
        environment={
            "POSTGRES_USER": PG_USER,
            "POSTGRES_PASSWORD": PG_PASSWORD,
            "POSTGRES_DB": "postgres",
        },
    )

    wait_for_postgres(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname="postgres",
    )

    yield

    container.remove(force=True)


@pytest.fixture(scope="session", autouse=True)
def init_db(postgres_container):
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname="postgres",
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE {PG_DATABASE}")
    cur.close()
    conn.close()

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE,
    )
    cur = conn.cursor()
    for schema in SCHEMAS:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.commit()
    cur.close()
    conn.close()


@pytest.fixture
def pg_con():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE,
    )
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture
def stub_page_log():
    yield PageLog(
        page=1,
        storage_info=StorageInfo(
            location="data/imports",
            format="json",
            file_name="logement.logements_maison_et_residences_principales_1.json",
            encoding="utf-8",
        ),
        is_last=False,
        success=True,
    )

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
