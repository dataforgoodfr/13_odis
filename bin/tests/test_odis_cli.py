from bin.odis import explain, extract
from common.data_source_model import DataSourceModel
from common.tests.stubs.source_extractor_stub import StubExtractor

from .stubs.handler import StubDataHandler


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
    explain(DataSourceModel(**dict_config))

    # then
    # no exception raised
    assert True


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
    explain(config, apis=apis)

    # then
    # no exception raised
    assert True


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
    explain(config, models=models)

    # then
    # no exception raised
    assert True


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

    data_handler = StubDataHandler()

    # verify create_extractor is called
    mocker.patch(
        "bin.odis.create_extractor",
        return_value=StubExtractor(
            config, list(config.get_models().values())[0], data_handler, data_handler
        ),
    )

    # when
    extract(config, config)

    # then
    # no exception raised but handler is not called
    assert data_handler.is_called
