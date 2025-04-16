import os

import psycopg2

from common.data_source_model import DataSourceModel
from common.tests.stubs.file_handler_stub import FileHandlerForCSVStub
from common.utils.database_client import DatabaseClient
from common.utils.interfaces.data_handler import PageLog
from common.utils.loaders.csv_loader import CsvDataLoader


def test_load_csv_data_nominal(
    pg_con: psycopg2.extensions.connection, pg_settings: dict, mocker
):
    """when everything is ok, the data is loaded in the database"""

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

    file_handler = FileHandlerForCSVStub(
        test_data_dir=os.path.split(os.path.abspath(__file__))[0] + "/data",
        file_name="test_data.csv",
    )

    data_loader = CsvDataLoader(
        model=model,
        config=config,
        db_client=DatabaseClient(
            settings=pg_settings,
            autocommit=False,
        ),
        handler=file_handler,
    )

    # need to create the table before loading data
    data_loader.create_or_overwrite_table()

    # make sure the table is empty before loading data
    # Clean up the test data
    cur = pg_con.cursor()
    cur.execute(f"DELETE FROM bronze.{model.table_name};")
    pg_con.commit()

    # when
    result = next(data_loader.load_data(file_handler.page_logs))

    # then
    assert isinstance(result, PageLog)
    assert result.success is True

    sql = f"""
        SELECT count(*)
        FROM bronze.{model.table_name}
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
                    "model_csv": {
                        "API": "api1",  # OK, api1 is defined
                        "type": "JsonExtractor",
                        "endpoint": "/geo/regions",
                        "description": "test",
                    },
                }
            },
        }
    )

    model = config.get_model("domain1.model_csv")
    data_loader = CsvDataLoader(
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
                "format": "csv",
                "file_name": "test_data.csv",
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

    file_handler = FileHandlerForCSVStub(
        test_data_dir=os.path.split(os.path.abspath(__file__))[0] + "/data",
        file_name="test_data.csv",
    )

    data_loader = CsvDataLoader(
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
    assert len(result) > 0

    assert any(
        col.name == "DEPARTEMENT_CODE".lower()
        # and col.description == "Code du département"
        for col in result
    )
