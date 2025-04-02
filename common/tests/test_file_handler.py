import os
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

from common.data_source_model import DataSourceModel
from common.utils.exceptions import InvalidCSV
from common.utils.file_handler import FileHandler
from common.utils.interfaces.data_handler import PageLog, StorageInfo


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


def test_file_name_when_provided_at_init():
    # given
    file_name = "john.doe"
    file_handler = FileHandler(file_name=file_name)
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

    # when
    _file_name = file_handler.file_name(model)

    # then
    assert _file_name == file_name


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


def test_handle_dict_data_with_csv_format():
    """
    case where we pass a dictionary as data, it should be saved as a json file
    even if the format is csv in the model
    (this is the case for metadata files)
    """
    mock_open_func = mock_open()

    # given
    file_name = "test_file.json"
    file_handler = FileHandler(file_name=file_name)  # file name is provided
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
    dict_data = {"data": "some data"}

    # when
    with patch("builtins.open", mock_open_func):
        storage_info = file_handler.file_dump(model, dict_data)

    # then
    assert storage_info.file_name == file_name


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
    df = file_handler.csv_load(page_log, model)

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
    df = file_handler.csv_load(page_log, model)

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
    with pytest.raises(InvalidCSV) as e:
        file_handler.csv_load(page_log, model)

    # then
    assert "blah" in str(e.value)
