import pytest
from pydantic_core import ValidationError

from ..data_source_model import APIModel, DataSourceModel, DomainModel


def test_EndPoint_may_be_slash():

    # given
    path = "/"

    # when
    model = DomainModel(API="INSEE.Metadonnees", type="JsonExtractor", endpoint=path)

    # then
    assert model.endpoint == path  # no exception, this is OK


def test_EndPoint_must_start_with_slash():

    # given
    path = "geo/regions"  # missing leading '/'

    # when
    with pytest.raises(ValidationError) as e:
        DomainModel(API="INSEE.Metadonnees", type="JsonExtractor", endpoint=path)

    # then
    assert "endpoint" in str(e.value)


def test_EndPoint_must_have_min_length():

    # given
    path = ""  # empty string

    # when
    with pytest.raises(ValidationError) as e:
        DomainModel(API="INSEE.Metadonnees", type="JsonExtractor", endpoint=path)

    # then
    assert "endpoint" in str(e.value)


def test_APIModel():
    # given

    model_dict = {
        "name": "INSEE.Metadonnees",
        "base_url": "https://api.insee.fr/",
        "apidoc": "https://api.insee.fr/doc/",
        "description": "API de métadonnées INSEE",
        "default_headers": {"accept": "application/json"},
    }

    # when
    model = APIModel(**model_dict)

    # then
    assert model.name == model_dict["name"]
    assert str(model.base_url) == model_dict["base_url"]
    assert str(model.apidoc) == model_dict["apidoc"]
    assert model.description == model_dict["description"]
    assert model.default_headers.accept == model_dict["default_headers"]["accept"]


def test_DomainModel():
    # given

    model_dict = {
        "API": "INSEE.Metadonnees",
        "description": "Référentiel géographique INSEE - niveau régional",
        "type": "JsonExtractor",
        "endpoint": "/geo/regions",
    }

    # when
    model = DomainModel(**model_dict)

    # then
    assert model.API == model_dict["API"]
    assert model.description == model_dict["description"]
    assert model.type == model_dict["type"]
    assert model.endpoint == model_dict["endpoint"]
    assert model.params is None


def test_DomainModel_bad_endpoint():
    # given-
    model_dict = {
        "API": "INSEE.Metadonnees",
        "type": "JsonExtractor",
        "endpoint": "geo/regions",  # missing leading '/'
    }

    # when
    with pytest.raises(ValidationError) as e:
        DomainModel(**model_dict)

    # then
    assert "endpoint" in str(e.value)


@pytest.mark.skip(reason="no check on the type for now")
def test_DomainModel_bad_type():

    # given-
    model_dict = {
        "API": "INSEE.Metadonnees",
        "type": "blah",  # invalid type
        "endpoint": "/geo/regions",
    }

    # when
    with pytest.raises(ValidationError) as e:
        DomainModel(**model_dict)

    # then
    assert "endpoint" in str(e.value)


def test_DataSourceModel_domain_api_is_ok():

    # given-
    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
        },
        "domains": {
            "level1": {
                "domain1": {
                    "API": "api1",  # OK, api1 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
            }
        },
    }

    # when
    m = DataSourceModel(**model_dict)

    # then
    assert m is not None


def test_DataSourceModel_domain_api_is_not_referenced():
    """to avoid using a domain that is not referenced in the APIs section"""

    # given-
    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
        },
        "domains": {
            "level1": {
                "domain1": {
                    "API": "api2",  # invalid API, not defined in the APIs section
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
            }
        },
    }

    # when
    with pytest.raises(ValidationError) as e:
        DataSourceModel(**model_dict)

    # then
    assert "api2" in str(e.value)
