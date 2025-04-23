import datetime
import os
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

from common.data_source_model import DataSourceModel
from common.utils.file_handler import FileHandler, ImporterException
from common.utils.interfaces.data_handler import OperationType, PageLog, StorageInfo


def test_file_name():
    # given
    file_handler = FileHandler()
    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
        },
        "domains": {
            "level1": {
                "mod1_lvl1": {
                    "API": "api1",  # OK, api1 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
            }
        },
    }

    m = DataSourceModel(**model_dict)
    model = list(m.get_models("level1").values())[0]
    model_name = model.name

    # when
    file_name = file_handler.file_name(model)

    # then
    assert file_name == f"{model_name}_1.json"


def test_file_name_format_csv():
    # given
    file_handler = FileHandler()
    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
        },
        "domains": {
            "level1": {
                "mod1_lvl1": {
                    "API": "api1",  # OK, api1 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                    "format": "csv",
                },
            }
        },
    }

    m = DataSourceModel(**model_dict)
    model = list(m.get_models("level1").values())[0]
    model_name = model.name

    # when
    file_name = file_handler.file_name(model)

    # then
    assert file_name == f"{model_name}_1.csv"


def test_file_name_increment():
    # given
    file_handler = FileHandler()
    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
        },
        "domains": {
            "level1": {
                "mod1_lvl1": {
                    "API": "api1",  # OK, api1 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
            }
        },
    }

    m = DataSourceModel(**model_dict)
    model = list(m.get_models("level1").values())[0]
    model_name = model.name
    file_handler.file_name(model)  # call once to increment the index

    # when
    file_name = file_handler.file_name(model)

    # then
    assert file_name == f"{model_name}_2.json"


def test_dump_metadata_with_csv_format(stub_page_log):
    mock_open_func = mock_open()

    # given
    file_handler = FileHandler()  # file name is provided
    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
        },
        "domains": {
            "level1": {
                "mod1_lvl1": {
                    "API": "api1",
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                    "format": "csv",  # csv format for the model
                },
            }
        },
    }

    m = DataSourceModel(**model_dict)
    model = list(m.get_models("level1").values())[0]
    pages = [stub_page_log]

    # when
    with patch("builtins.open", mock_open_func):
        meta_info = file_handler.dump_metadata(
            model,
            OperationType.EXTRACT,
            start_time=datetime.datetime.now(tz=datetime.timezone.utc),
            last_processed_page=1,
            complete=True,
            errors=0,
            pages=pages,
            artifacts=[],
        )

    # then
    assert meta_info.operation == OperationType.EXTRACT


def test_handle_csv():
    """ """
    mock_open_func = mock_open()

    # given
    file_handler = FileHandler()  # file name is provided
    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
        },
        "domains": {
            "level1": {
                "mod1_lvl1": {
                    "API": "api1",
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                    "format": "csv",  # csv format for the model
                },
            }
        },
    }

    m = DataSourceModel(**model_dict)
    model = list(m.get_models("level1").values())[0]
    csv_data = "data1,data2\n1,2"  # csv data

    # when
    with patch("builtins.open", mock_open_func):
        storage_info = file_handler.file_dump(model, csv_data)

    # then
    assert Path(storage_info.file_name).suffix == ".csv"


def test_handle_binary_data():
    """
    when we pass binary data for json model, it should be decoded and saved as a json file
    """
    mock_open_func = mock_open()

    # given
    file_handler = FileHandler()  # file name is provided
    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
        },
        "domains": {
            "level1": {
                "mod1_lvl1": {
                    "API": "api1",
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                    "format": "json",  # json format for the model
                },
            }
        },
    }

    m = DataSourceModel(**model_dict)
    model = list(m.get_models("level1").values())[0]
    csv_data = b"some binary data"

    # when
    with patch("builtins.open", mock_open_func):
        storage_info = file_handler.file_dump(model, csv_data)

    # then
    assert Path(storage_info.file_name).suffix == ".json"


def test_load_csv_coma_separated():
    """ """

    # given
    file_handler = FileHandler()  # file name is provided
    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
        },
        "domains": {
            "level1": {
                "mod1_lvl1": {
                    "API": "api1",
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                    "format": "csv",
                    "load_params": {"separator": ","},  # comma as separator
                },
            }
        },
    }

    m = DataSourceModel(**model_dict)
    model = m.get_model("level1.mod1_lvl1")

    test_data_dir = os.path.split(os.path.abspath(__file__))[0] + "/data"
    page_log = PageLog(
        page=1,
        storage_info=StorageInfo(
            location=test_data_dir,
            format="csv",
            file_name="test_coma_sep.csv",
            encoding="utf-8",
        ),
    )

    # when
    df = file_handler.csv_load(page_log.storage_info, model)

    # then
    assert df.shape == (2, 2)
    assert df.columns.tolist() == ["data_1", "data_2"]


def test_load_csv_semicolon_separated():
    """ """
    # given
    file_handler = FileHandler()  # file name is provided
    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
        },
        "domains": {
            "level1": {
                "mod1_lvl1": {
                    "API": "api1",
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                    "format": "csv",  # csv format for the model
                    "load_params": {"separator": ";"},  # semi-column as separator
                },
            }
        },
    }

    m = DataSourceModel(**model_dict)
    model = m.get_model("level1.mod1_lvl1")

    test_data_dir = os.path.split(os.path.abspath(__file__))[0] + "/data"
    page_log = PageLog(
        page=1,
        storage_info=StorageInfo(
            location=test_data_dir,
            format="csv",
            file_name="test_semicol_sep.csv",
            encoding="utf-8",
        ),
    )

    # when
    df = file_handler.csv_load(page_log.storage_info, model)

    # then
    assert df.shape == (2, 2)
    assert df.columns.tolist() == ["data_1", "data_2"]


def test_load_csv_raises_invalid_csv():
    """ """
    # given
    file_handler = FileHandler()  # file name is provided
    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
        },
        "domains": {
            "level1": {
                "mod1_lvl1": {
                    "API": "api1",
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                    "format": "csv",  # csv format for the model
                    "load_params": {"separator": ";"},  # semi-column as separator
                },
            }
        },
    }

    m = DataSourceModel(**model_dict)
    model = m.get_model("level1.mod1_lvl1")

    test_data_dir = os.path.split(os.path.abspath(__file__))[0] + "/data"
    page_log = PageLog(
        page=1,
        storage_info=StorageInfo(
            location=test_data_dir,
            format="csv",
            file_name="blah",  # invalid file name
            encoding="utf-8",
        ),
    )

    # when
    with pytest.raises(ImporterException) as e:
        file_handler.csv_load(page_log.storage_info, model)

    # then
    assert "blah" in str(e.value)


# def test_read_large_page():
#     # given
#     test_data_dir = os.path.split(os.path.abspath(__file__))[0] + "/data"
#     file_handler = FileHandler()

#     page = PageLog(
#         page=1,
#         storage_info=StorageInfo(
#             location=test_data_dir,
#             format="json",
#             file_name="test_large_json.json",
#             encoding="utf-8",
#         ),
#     )

#     content = []

#     # when
#     for last_line in file_handler.read_pages([page]):
#         content.append(last_line)

#     # then
#     assert len(content) == 1  # Ensure that only one chunk was read

#     assert all(
#         [isinstance(line, dict) for line in content]
#     )  # Ensure that the content is a list of dictionaries


# def test_read_several_pages():
#     # given
#     test_data_dir = os.path.split(os.path.abspath(__file__))[0] + "/data"
#     file_handler = FileHandler()

#     page_1 = PageLog(
#         page=1,
#         storage_info=StorageInfo(
#             location=test_data_dir,
#             format="json",
#             file_name="test_array_of_json.json",
#             encoding="utf-8",
#         ),
#     )
#     page_2 = PageLog(
#         page=2,
#         storage_info=StorageInfo(
#             location=test_data_dir,
#             format="json",
#             file_name="test_data.json",
#             encoding="utf-8",
#         ),
#     )

#     content = []

#     # when
#     for last_line in file_handler.read_pages([page_1, page_2]):
#         content.append(last_line)

#     # then
#     assert len(content) > 0  # Ensure that two chunks were read


# def test_read_several_inconsistent_pages():
#     pass
