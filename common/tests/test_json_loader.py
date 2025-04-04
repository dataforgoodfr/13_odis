import os

import psycopg2

from common.data_source_model import DataSourceModel
from common.tests.stubs.file_handler_stub import FileHandlerForJsonStub
from common.utils.database_client import DatabaseClient
from common.utils.interfaces.data_handler import PageLog
from common.utils.loaders.json_loader import JsonDataLoader


def test_create_or_overwrite_json_table(
    pg_con: psycopg2.extensions.connection, pg_settings: dict
):

    # given
    config = DataSourceModel(
        **{
            "APIs": {
                "api1": {
                    "name": "INSEE.Metadonnees",
                    "base_url": "https://api.insee.fr/",
                    "default_headers": {"accept": "application/xml"},  # default headers
                },
            },
            "domains": {
                "domain1": {
                    "model_json": {
                        "API": "api1",  # OK, api1 is defined
                        "type": "JsonExtractor",
                        "endpoint": "/geo/regions",
                        "description": "test",
                    },
                }
            },
        }
    )

    model = config.get_model("domain1.model_json")
    data_loader = JsonDataLoader(
        model=model,
        config=config,
        db_client=DatabaseClient(
            settings=pg_settings,
            autocommit=False,
        ),
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


def test_load_json_data_nominal(
    pg_con: psycopg2.extensions.connection, pg_settings: dict
):
    """when everything is ok, the data is loaded in the database"""

    # given
    config = DataSourceModel(
        **{
            "APIs": {
                "api1": {
                    "name": "INSEE.Metadonnees",
                    "base_url": "https://api.insee.fr/",
                    "default_headers": {"accept": "application/xml"},  # default headers
                },
            },
            "domains": {
                "domain1": {
                    "model_json": {
                        "API": "api1",  # OK, api1 is defined
                        "type": "JsonExtractor",
                        "endpoint": "/geo/regions",
                        "description": "test",
                    },
                }
            },
        }
    )

    model = config.get_model("domain1.model_json")
    handler = FileHandlerForJsonStub()

    data_loader = JsonDataLoader(
        model=model,
        config=config,
        db_client=DatabaseClient(
            settings=pg_settings,
            autocommit=False,
        ),
        handler=handler,
    )

    # need to create the table before loading data
    data_loader.create_or_overwrite_table()

    # make sure the table is empty before loading data
    # Clean up the test data
    cur = pg_con.cursor()
    cur.execute(f"DELETE FROM bronze.{model.table_name};")
    pg_con.commit()

    # when
    result = next(data_loader.load_data(handler.page_logs))

    # then
    assert isinstance(result, PageLog)
    assert result.success is True

    sql = f"""
        SELECT count(*)
        FROM bronze.{model.table_name}
        WHERE data IS NOT NULL;
    """

    cur.execute(sql)

    assert cur.fetchone()[0] > 0  # Check if the table has data

    # Tear down
    # Clean up the test data
    cur.execute(f"DELETE FROM bronze.{model.table_name};")
    pg_con.commit()


def test_load_data_array_of_json(
    pg_con: psycopg2.extensions.connection, pg_settings: dict
):
    """the data is an array of JSON objects"""

    # given
    config = DataSourceModel(
        **{
            "APIs": {
                "api1": {
                    "name": "INSEE.Metadonnees",
                    "base_url": "https://api.insee.fr/",
                    "default_headers": {"accept": "application/xml"},  # default headers
                },
            },
            "domains": {
                "domain1": {
                    "model_json": {
                        "API": "api1",  # OK, api1 is defined
                        "type": "JsonExtractor",
                        "endpoint": "/geo/regions",
                        "description": "test",
                    },
                }
            },
        }
    )

    test_data_dir = os.path.split(os.path.abspath(__file__))[0] + "/data"

    handler = FileHandlerForJsonStub(
        page_logs=[
            PageLog(
                **{
                    "page": 1,
                    "success": True,
                    "is_last": True,
                    "storage_info": {
                        "location": test_data_dir,
                        "format": "json",
                        "file_name": "test_array_of_json.json",
                        "encoding": "utf-8",
                    },
                }
            )
        ]
    )

    model = config.get_model("domain1.model_json")
    data_loader = JsonDataLoader(
        model=model,
        config=config,
        db_client=DatabaseClient(
            settings=pg_settings,
            autocommit=False,
        ),
        handler=handler,
    )

    # need to create the table before loading data
    data_loader.create_or_overwrite_table()

    # make sure the table is empty before loading data
    # Clean up the test data
    cur = pg_con.cursor()
    cur.execute(f"DELETE FROM bronze.{model.table_name};")
    pg_con.commit()

    # when
    result = next(data_loader.load_data(handler.page_logs))

    # then
    assert isinstance(result, PageLog)
    assert result.success is True

    sql = f"""
        SELECT count(*)
        FROM bronze.{model.table_name}
        WHERE data IS NOT NULL;
    """

    cur.execute(sql)

    assert cur.fetchone()[0] > 0  # Check if the table has data

    # Tear down
    # Clean up the test data
    cur.execute(f"DELETE FROM bronze.{model.table_name};")
    pg_con.commit()


def test_load_data_raises_error_if_table_does_not_exist(
    pg_con: psycopg2.extensions.connection, pg_settings: dict
):
    """scenario where the table does not exist in the database"""

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
                    "model_json": {
                        "API": "api1",  # OK, api1 is defined
                        "type": "JsonExtractor",
                        "endpoint": "/geo/regions",
                        "description": "test",
                    },
                }
            },
        }
    )

    model = config.get_model("domain1.model_json")
    data_loader = JsonDataLoader(
        model=model,
        config=config,
        db_client=DatabaseClient(
            settings=pg_settings,
            autocommit=False,
        ),
    )
    test_data_dir = os.path.split(os.path.abspath(__file__))[0] + "/data"
    pagelog = PageLog(
        **{
            "page": 1,
            "success": True,
            "is_last": True,
            "storage_info": {
                "location": test_data_dir,
                "format": "json",
                "file_name": "test_data.json",
                "encoding": "utf-8",
            },
        }
    )

    # make sure the table does not exist before loading data
    cur = pg_con.cursor()
    cur.execute(f"DROP TABLE IF EXISTS bronze.{model.table_name}")
    pg_con.commit()

    # when
    result = next(data_loader.load_data([pagelog]))

    # then
    assert not result.success


def test_load_data_raises_error_when_data_is_corrupt(
    pg_con: psycopg2.extensions.connection, pg_settings: dict
):
    """scenario where the data is not JSON valid"""

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
                    "model_json": {
                        "API": "api1",  # OK, api1 is defined
                        "type": "JsonExtractor",
                        "endpoint": "/geo/regions",
                        "description": "test",
                    },
                }
            },
        }
    )

    model = config.get_model("domain1.model_json")
    data_loader = JsonDataLoader(
        model=model,
        config=config,
        db_client=DatabaseClient(
            settings=pg_settings,
            autocommit=False,
        ),
    )
    test_data_dir = os.path.split(os.path.abspath(__file__))[0] + "/data"
    pagelog = PageLog(
        **{
            "page": 1,
            "success": True,
            "is_last": True,
            "storage_info": {
                "location": test_data_dir,
                "format": "json",
                "file_name": "test_bad_data.json",  # this file is not valid JSON
                "encoding": "utf-8",
            },
        }
    )

    # need to create the table before loading data
    data_loader.create_or_overwrite_table()

    # when
    result = next(data_loader.load_data([pagelog]))

    # then
    assert not result.success


def test_list_columns(pg_settings):
    """scenario where the table does not exist in the database"""

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
                    "model_json": {
                        "API": "api1",  # OK, api1 is defined
                        "type": "JSONExtractor",
                        "endpoint": "/geo/regions",
                        "description": "test",
                    },
                }
            },
            "dictionary": {
                "domain1": {
                    "model_json": {
                        "DEPARTEMENT_CODE": "Code du département",
                    }
                },
            },
        }
    )

    model = config.get_model("domain1.model_json")

    file_handler = FileHandlerForJsonStub()

    data_loader = JsonDataLoader(
        model=model,
        config=config,
        db_client=DatabaseClient(
            settings=pg_settings,
            autocommit=False,
        ),
        handler=file_handler,
    )

    # when
    result = data_loader.list_columns()

    # then
    assert len(result) == 1
    assert result[0].name == "data"
    assert result[0].data_type == "JSONB"
    assert result[0].description == "DEPARTEMENT_CODE: Code du département"
