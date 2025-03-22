import pytest

from bin.extract import create_extractor, explain_source, extract_data
from common.data_source_model import DataSourceModel
from common.tests.stubs.source_extractor_stub import StubExtractor
from common.utils.interfaces.extractor import AbstractSourceExtractor
from common.utils.source_extractors import JsonExtractor

from .stubs.handler import StubDataHandler


def test_create_extractor_nominal():
    # given
    dict_config = {
        "APIs": {
            "INSEE.Metadonnees": {
                "name": "Metadonnees INSEE",
                "base_url": "https://geo.api.gouv.fr",
            }
        },
        "domains": {
            "INSEE": {
                "Metadonnees": {
                    "API": "INSEE.Metadonnees",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                }
            }
        },
    }
    config = DataSourceModel(**dict_config)
    model = list(config.get_models().values())[0]

    # when
    extractor = create_extractor(config, model)

    # then
    assert extractor is not None
    assert isinstance(extractor, JsonExtractor)


def test_create_extractor_with_error():
    # given
    dict_config = {
        "APIs": {
            "INSEE.Metadonnees": {
                "name": "Metadonnees INSEE",
                "base_url": "https://geo.api.gouv.fr",
            }
        },
        "domains": {
            "INSEE": {
                "Metadonnees": {
                    "API": "INSEE.Metadonnees",
                    "type": "DummyExtractor",  # wrong type
                    "endpoint": "/geo/regions",
                }
            }
        },
    }
    config = DataSourceModel(**dict_config)
    model = list(config.get_models().values())[0]

    # when
    with pytest.raises(ValueError) as e:
        create_extractor(config, model)

    # then
    assert "DummyExtractor" in str(e)


def test_explain_source_generic_explanations():
    """just validate there is no error"""

    # given
    dict_config = {
        "APIs": {
            "INSEE.Metadonnees": {
                "name": "Metadonnees INSEE",
                "base_url": "https://geo.api.gouv.fr",
            }
        },
        "domains": {
            "INSEE": {
                "Metadonnees": {
                    "API": "INSEE.Metadonnees",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                }
            }
        },
    }

    # when
    explanations = explain_source(DataSourceModel(**dict_config))

    # then
    assert explanations is not None
    assert isinstance(explanations, str)


def test_explain_source_apis():
    # given
    dict_config = {
        "APIs": {
            "INSEE.Metadonnees": {
                "name": "Metadonnees INSEE",
                "base_url": "https://geo.api.gouv.fr",
            }
        },
        "domains": {
            "INSEE": {
                "Metadonnees": {
                    "API": "INSEE.Metadonnees",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                }
            }
        },
    }
    config = DataSourceModel(**dict_config)
    apis = list(config.APIs.values())

    # when
    explanations = explain_source(config, apis=apis)

    # then
    assert explanations is not None
    assert isinstance(explanations, str)


def test_explain_source_models():
    """verify there is no error when explaining models"""
    # given

    dict_config = {
        "APIs": {
            "INSEE.Metadonnees": {
                "name": "Metadonnees INSEE",
                "base_url": "https://geo.api.gouv.fr",
            }
        },
        "domains": {
            "INSEE": {
                "Metadonnees": {
                    "API": "INSEE.Metadonnees",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                }
            }
        },
    }
    config = DataSourceModel(**dict_config)
    models = list(config.get_models().values())

    # when
    explanations = explain_source(config, models=models)

    # then
    assert explanations is not None
    assert isinstance(explanations, str)


def test_create_extractor():
    # given
    dict_config = {
        "APIs": {
            "INSEE.Metadonnees": {
                "name": "Metadonnees INSEE",
                "base_url": "https://geo.api.gouv.fr",
            }
        },
        "domains": {
            "INSEE": {
                "Metadonnees": {
                    "API": "INSEE.Metadonnees",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                }
            }
        },
    }
    config = DataSourceModel(**dict_config)
    model = list(config.get_models().values())[0]

    # when
    extractor = create_extractor(config, model)

    # then
    assert extractor is not None
    assert isinstance(extractor, AbstractSourceExtractor)


def test_extract_data(mocker):
    # given

    dict_config = {
        "APIs": {
            "INSEE": {
                "name": "Metadonnees INSEE",
                "base_url": "https://geo.api.gouv.fr",
            }
        },
        "domains": {
            "INSEE": {
                "Metadonnees": {
                    "API": "INSEE",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                }
            }
        },
    }
    config = DataSourceModel(**dict_config)

    # verify create_extractor is called
    mocker.patch(
        "bin.extract.create_extractor",
        return_value=StubExtractor(config, list(config.get_models().values())[0]),
    )
    data_handler = StubDataHandler()

    # when
    extract_data(config, config)

    # then
    # no exception raised but handler is not called
    assert not data_handler.is_called
