import os

import psycopg2

from common.data_source_model import DataSourceModel
from common.tests.stubs.data_loader_stub import CommentedDataLoader
from common.tests.stubs.file_handler_stub import FileHandlerForCSVStub
from common.utils.database_client import DatabaseClient
from common.utils.interfaces.loader import Column, ColumnType


def test_Column_name_is_sanitized():
    # given
    name = " Col ?# _ '\" "

    # when
    col = Column(name=name, data_type=ColumnType.JSON, description="test")

    # then
    assert col.name == "col"


def test_create_or_overwrite_table(
    pg_con: psycopg2.extensions.connection, pg_settings: dict, mocker
):

    # given
    config = DataSourceModel(
        **{
            "APIs": {
                "api1": {
                    "name": "INSEE.Metadonnees",
                    "base_url": "https://api.insee.fr/",
                },
            },
            "domains": {
                "domain1": {
                    "model_csv": {
                        "API": "api1",  # OK, api1 is defined
                        "type": "CSVExtractor",
                        "endpoint": "/geo/regions",
                        "description": "test",
                    },
                }
            },
        }
    )

    model = config.get_model("domain1.model_csv")
    data_loader = CommentedDataLoader(
        model=model,
        config=config,
        db_client=DatabaseClient(
            settings=pg_settings,
            autocommit=False,
        ),
        handler=FileHandlerForCSVStub(
            test_data_dir=os.path.split(os.path.abspath(__file__))[0] + "/data",
            file_name="test_data.csv",
        ),
        col_name="col",
        col_description="test column description",
    )

    # when
    data_loader.create_or_overwrite_table()

    # then
    # Check if the table was created successfully
    cur = pg_con.cursor()

    sql = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'bronze'
            AND    table_name   = '{model.table_name}'
        );
    """

    cur.execute(sql)

    assert cur.fetchone()[0]  # Check if the table exists


def test_create_or_overwrite_table_comment_on_table(
    pg_con: psycopg2.extensions.connection, pg_settings: dict, mocker
):
    # given
    table_description = "test table description"
    config = DataSourceModel(
        **{
            "APIs": {
                "api1": {
                    "name": "INSEE.Metadonnees",
                    "base_url": "https://api.insee.fr/",
                },
            },
            "domains": {
                "domain1": {
                    "model_csv": {
                        "API": "api1",
                        "type": "CSVExtractor",
                        "endpoint": "/geo/regions",
                        "description": table_description,  # table description
                    },
                }
            },
        }
    )

    model = config.get_model("domain1.model_csv")
    data_loader = CommentedDataLoader(
        model=model,
        config=config,
        db_client=DatabaseClient(
            settings=pg_settings,
            autocommit=False,
        ),
        handler=FileHandlerForCSVStub(
            test_data_dir=os.path.split(os.path.abspath(__file__))[0] + "/data",
            file_name="test_data.csv",
        ),
        col_name="col",
        col_description="test column description",
    )

    # when
    data_loader.create_or_overwrite_table()

    # then
    # Check if the table was created successfully
    cur = pg_con.cursor()

    sql = f"""
        SELECT pg_catalog.obj_description(pgc.oid, 'pg_class')
        FROM information_schema.tables t
        INNER JOIN pg_catalog.pg_class pgc
        ON t.table_name = pgc.relname 
        WHERE 
        t.table_name='{model.table_name}'
        AND t.table_schema='bronze';
    """

    cur.execute(sql)

    desc = cur.fetchone()[0]

    assert desc == table_description  # Check if the table description is correct


def test_create_or_overwrite_table_comment_on_columns(
    pg_con: psycopg2.extensions.connection, pg_settings: dict, mocker
):

    # given
    table_description = "test table description"
    config = DataSourceModel(
        **{
            "APIs": {
                "api1": {
                    "name": "INSEE.Metadonnees",
                    "base_url": "https://api.insee.fr/",
                },
            },
            "domains": {
                "domain1": {
                    "model_csv": {
                        "API": "api1",
                        "type": "CSVExtractor",
                        "endpoint": "/geo/regions",
                        "description": table_description,  # table description
                    },
                }
            },
        }
    )

    model = config.get_model("domain1.model_csv")
    data_loader = CommentedDataLoader(
        model=model,
        config=config,
        db_client=DatabaseClient(
            settings=pg_settings,
            autocommit=False,
        ),
        handler=FileHandlerForCSVStub(
            test_data_dir=os.path.split(os.path.abspath(__file__))[0] + "/data",
            file_name="test_data.csv",
        ),
        col_name="col",
        col_description="test column description",
    )

    # when
    data_loader.create_or_overwrite_table()

    # then
    # Check if the table was created successfully
    cur = pg_con.cursor()

    sql = f"""
        SELECT 
            pgd.description as column_description
        FROM pg_class
        INNER JOIN
            information_schema.columns
                ON table_name = pg_class.relname
        LEFT JOIN 
            pg_catalog.pg_description pgd
                ON pgd.objsubid = ordinal_position
        WHERE 
            relname = '{model.table_name}'
    """

    cur.execute(sql)

    desc = cur.fetchone()[0]

    assert (
        desc == data_loader.col_description
    )  # Check if the column description is correct


def test_create_or_overwrite_table_comment_none_on_column(
    pg_con: psycopg2.extensions.connection, pg_settings: dict, mocker
):
    """case where the column description is None"""

    # given
    table_description = "test table description"
    config = DataSourceModel(
        **{
            "APIs": {
                "api1": {
                    "name": "INSEE.Metadonnees",
                    "base_url": "https://api.insee.fr/",
                },
            },
            "domains": {
                "domain1": {
                    "model_csv": {
                        "API": "api1",
                        "type": "CSVExtractor",
                        "endpoint": "/geo/regions",
                        "description": table_description,  # table description
                    },
                }
            },
        }
    )

    model = config.get_model("domain1.model_csv")
    data_loader = CommentedDataLoader(
        model=model,
        config=config,
        db_client=DatabaseClient(
            settings=pg_settings,
            autocommit=False,
        ),
        handler=FileHandlerForCSVStub(
            test_data_dir=os.path.split(os.path.abspath(__file__))[0] + "/data",
            file_name="test_data.csv",
        ),
        col_name="col",
        col_description=None,  # column description is None
    )

    # when
    data_loader.create_or_overwrite_table()

    # then
    # Check if the table was created successfully
    cur = pg_con.cursor()

    sql = f"""
        SELECT 
            pgd.description as column_description
        FROM pg_class
        INNER JOIN
            information_schema.columns
                ON table_name = pg_class.relname
        LEFT JOIN 
            pg_catalog.pg_description pgd
                ON pgd.objsubid = ordinal_position
        WHERE 
            relname = '{model.table_name}'
    """

    cur.execute(sql)

    desc = cur.fetchone()[0]

    assert (
        desc == data_loader.col_description
    )  # Check if the column description is correct
