from bin.extract import explain_source
from common.data_source_model import DataSourceModel


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


def test_explain_source_domain():
    """just validate there is no error when explaining a domain"""

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
    explanations = explain_source(DataSourceModel(**dict_config), domain="INSEE")

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

    # when
    explanations = explain_source(
        DataSourceModel(**dict_config), apis=["INSEE.Metadonnees"]
    )

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

    # when
    explanations = explain_source(
        DataSourceModel(**dict_config), models=["Metadonnees"]
    )

    # then
    assert explanations is not None
    assert isinstance(explanations, str)
