import pytest

from common.data_source_model import DataSourceModel
from common.utils.factory.extractor_factory import create_extractor
from common.utils.source_extractors import JsonExtractor


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
                    "description": "Valid test description",
                }
            }
        },
    }
    config = DataSourceModel(**dict_config)
    model = list(config.get_models().values())[0]

    # when
    extractor = create_extractor(config, model, None)

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
                    "description": "Valid test description",
                }
            }
        },
    }
    config = DataSourceModel(**dict_config)
    model = list(config.get_models().values())[0]

    # when
    with pytest.raises(ValueError) as e:
        create_extractor(config, model, None)

    # then
    assert "DummyExtractor" in str(e)
