import tempfile

import pytest
from pydantic_core import ValidationError

from ..data_source_model import (
    APIModel,
    DataProcessingParameters,
    DataSourceModel,
    DomainModel,
    HeaderModel,
)


def test_EndPoint_may_be_slash():

    # given
    path = "/"

    # when
    model = DomainModel(
        API="INSEE.Metadonnees",
        type="JsonExtractor",
        description="Only / in the endpoint path",
        endpoint=path,
    )

    # then
    assert model.endpoint == path  # no exception, this is OK


def test_EndPoint_must_start_with_slash():

    # given
    path = "geo/regions"  # missing leading '/'

    # when
    with pytest.raises(ValidationError) as e:
        DomainModel(
            API="INSEE.Metadonnees",
            type="JsonExtractor",
            description="Endpoint must start with /",
            endpoint=path,
        )

    # then
    assert "endpoint" in str(e.value)


def test_EndPoint_must_have_min_length():

    # given
    path = ""  # empty string

    # when
    with pytest.raises(ValidationError) as e:
        DomainModel(
            API="INSEE.Metadonnees",
            type="JsonExtractor",
            description="Endpoint must have min length",
            endpoint=path,
        )

    # then
    assert "endpoint" in str(e.value)


def test_Description_must_have_min_length():

    # given
    description = ""  # empty string

    # when
    with pytest.raises(ValidationError) as e:
        DomainModel(
            API="INSEE.Metadonnees",
            type="JsonExtractor",
            description=description,
            endpoint="/example",
        )

    # then
    assert "description" in str(e.value)


def test_DomainModel_default_headers():
    # given

    # when
    model = DomainModel(
        API="INSEE.Metadonnees",
        type="JsonExtractor",
        description="This is for testing default headers",
        endpoint="/geo/regions",
    )

    # then
    assert model.headers.accept == "application/json"


def test_DomainModel_headers_are_merged_with_api_ones():
    # given
    model = DomainModel(
        API="INSEE.Metadonnees",
        type="JsonExtractor",
        description="Valid test description",
        endpoint="/geo/regions",
        headers=HeaderModel(another_key="another_value", accept="application/xml"),
    )

    # when
    model.merge_headers(HeaderModel(accept="application/json", api_key="api_value"))

    # then
    assert model.headers.accept == "application/xml"  # the domain model wins
    assert (
        model.headers.another_key == "another_value"
    )  # imported from the domain model
    assert model.headers.api_key == "api_value"  # imported from the API model


def test_DomainModel_notebook_name_is_mandatory_for_Preprocessor():
    # given
    domain_type = "FileExtractor"

    # when
    with pytest.raises(ValueError) as e:

        processor_info = DataProcessingParameters(type="notebook")

        DomainModel(
            type=domain_type,
            API="INSEE.Metadonnees",
            endpoint="/geo/regions",
            description="Référentiel géographique INSEE - niveau régional",
            preprocessor=processor_info,
        )

    # then
    assert "name" in str(e.value)


def test_DomainModel_preprocessor_nominal():
    # given
    domain_type = "FileExtractor"

    # when
    # create a temporary file
    # to simulate a notebook path
    # and check that the path is valid
    with tempfile.NamedTemporaryFile() as fp:

        processor_info = DataProcessingParameters(
            name=fp.name.split(".")[0], type="notebook"
        )

        m = DomainModel(
            type=domain_type,
            API="INSEE.Metadonnees",
            endpoint="/geo/regions",
            description="Référentiel géographique INSEE - niveau régional",
            preprocessor=processor_info,
        )

    # then
    assert m.preprocessor.name is not None


def test_DomainModel_API_is_mandatory():
    # given
    domain_type = "JsonExtractor"

    # when
    with pytest.raises(ValueError) as e:
        DomainModel(
            type=domain_type,
            API=None,
        )

    # then
    assert "API" in str(e.value)


def test_DomainModel_endpoint_is_mandatory():
    # given
    domain_type = "JsonExtractor"

    # when
    with pytest.raises(ValueError) as e:
        DomainModel(
            type=domain_type,
            API="INSEE.Metadonnees",
            endpoint=None,
        )

    # then
    assert "endpoint" in str(e.value)


def test_HeaderModel_accepts_extra_keys():
    # given

    # when
    model = HeaderModel(accept="application/json", extra_key="extra_value")

    # then
    assert model.accept == "application/json"
    assert model.extra_key == "extra_value"


def test_HeaderModel_default_accept():
    # given

    # when
    model = HeaderModel()

    # then
    assert model.accept == "application/json"


def test_HeaderModel_accept_xml():
    # given

    # when
    model = HeaderModel(accept="application/xml")

    # then
    assert model.accept == "application/xml"


def test_HeaderModel_accept_csv():
    # given

    # when
    model = HeaderModel(accept="text/csv")

    # then
    assert model.accept == "text/csv"


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
    assert model.throttle == 60


def test_APIModel_no_apidoc():
    # given

    model_dict = {
        "name": "INSEE.Metadonnees",
        "base_url": "https://api.insee.fr/",
        "description": "API de métadonnées INSEE",
        "default_headers": {"accept": "application/json"},
    }

    # when
    model = APIModel(**model_dict)

    # then
    assert model.apidoc is None


def test_APIModel_set_throttle():

    # given
    model_dict = {
        "name": "INSEE.Metadonnees",
        "base_url": "https://api.insee.fr/",
        "description": "API de métadonnées INSEE",
        "default_headers": {"accept": "application/json"},
        "throttle": 120,
    }

    # when
    model = APIModel(**model_dict)

    # then
    assert model.throttle == 120


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
    assert model.load_params is not None  # default value
    assert model.extract_params is None
    assert model.preprocessor is None


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


def test_DomainModel_load_params_is_arbitrary_dict():
    # given

    model_dict = {
        "API": "INSEE.Metadonnees",
        "type": "JsonExtractor",
        "description": "Valid test description",
        "endpoint": "/geo/regions",
        "load_params": {"key": "value", "key2": 1.2},
    }

    # when
    model = DomainModel(**model_dict)

    # then
    # check all keys are in the model
    # and values are kept as-is
    assert all(
        [
            k in model.load_params.model_dump(mode="json")
            and v == model.load_params.model_dump(mode="json")[k]
            for k, v in model_dict["load_params"].items()
        ]
    )


def test_DomainModel_extract_params_is_arbitrary_dict():
    # given

    model_dict = {
        "API": "INSEE.Metadonnees",
        "type": "JsonExtractor",
        "description": "Valid test description",
        "endpoint": "/geo/regions",
        "extract_params": {"key": "value", "key2": 1.2},
    }

    # when
    model = DomainModel(**model_dict)

    # then
    assert model.extract_params == model_dict["extract_params"]


def test_DomainModel_extract_params_default_value():
    # given

    model_dict = {
        "API": "INSEE.Metadonnees",
        "type": "JsonExtractor",
        "description": "Valid test description",
        "endpoint": "/geo/regions",
    }

    # when
    model = DomainModel(**model_dict)

    # then
    assert model.extract_params is None


def test_DomainModel_load_params_default_value():
    # given

    model_dict = {
        "API": "INSEE.Metadonnees",
        "type": "JsonExtractor",
        "description": "Valid test description",
        "endpoint": "/geo/regions",
    }

    # when
    model = DomainModel(**model_dict)

    # then
    assert model.load_params is not None
    assert model.load_params.model_dump()["separator"] == ";"
    assert model.load_params.model_dump()["header"] == 0
    assert model.load_params.model_dump()["skipfooter"] == 0


def test_DomainModel_preprocessor_params_default_value():
    # given

    model_dict = {
        "API": "INSEE.Metadonnees",
        "type": "JsonExtractor",
        "description": "Valid test description",
        "endpoint": "/geo/regions",
        "preprocessor": {"name": "bmo_2024"},
    }

    # when
    model = DomainModel(**model_dict)

    # then
    assert model.preprocessor.model_dump()["type"] == "notebook"


def test_DomainModel_response_map_is_arbitrary_dict():
    # given

    model_dict = {
        "API": "INSEE.Metadonnees",
        "type": "JsonExtractor",
        "description": "Valid test description",
        "endpoint": "/geo/regions",
        "response_map": {"next": "paging.next"},
    }

    # when
    model = DomainModel(**model_dict)

    # then
    assert model.response_map == model_dict["response_map"]


def test_DomainModel_response_map_default_value():

    # given
    model_dict = {
        "API": "INSEE.Metadonnees",
        "type": "JsonExtractor",
        "description": "Valid test description",
        "endpoint": "/geo/regions",
    }

    # when
    model = DomainModel(**model_dict)

    # then
    assert model.response_map == {}


def test_get_api_domains():
    # given

    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
            "api2": {
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
                "domain2": {
                    "API": "api2",  # OK, api2 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
                "domain3": {
                    "API": "api2",  # OK, api2 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
            }
        },
    }

    m = DataSourceModel(**model_dict)

    # when

    domains = m.get_domains_with_models_for_api("api1")

    # then
    assert domains == {"level1": ["domain1"]}


def test_get_api_domains_multiple():
    # given

    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
            "api2": {
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
                "domain2": {
                    "API": "api2",  # OK, api2 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
                "domain3": {
                    "API": "api2",  # OK, api2 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
            }
        },
    }

    m = DataSourceModel(**model_dict)

    # when

    domains = m.get_domains_with_models_for_api("api2")

    # then
    assert domains == {"level1": ["domain2", "domain3"]}


def test_list_domains():

    # given
    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
            "api2": {
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
                "domain2": {
                    "API": "api2",  # OK, api2 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
                "domain3": {
                    "API": "api2",  # OK, api2 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
            }
        },
    }

    m = DataSourceModel(**model_dict)

    # when
    domains = m.list_domains_names()

    # then
    assert domains == ["level1"]


def test_get_models():

    # given

    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
            "api2": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
        },
        "domains": {
            "level1": {
                "model1": {
                    "API": "api1",  # OK, api1 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
                "model2": {
                    "API": "api2",  # OK, api2 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
                "model3": {
                    "API": "api2",  # OK, api2 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
            }
        },
    }

    m = DataSourceModel(**model_dict)

    # when
    models = m.get_models()

    # then
    assert len(models) == 3
    assert "level1.model1" in models
    assert "level1.model2" in models
    assert "level1.model3" in models
    assert all([isinstance(v, DomainModel) for v in models.values()])


def test_get_models_by_domain():

    # given

    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
            "api2": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
        },
        "domains": {
            "level1": {
                "mod1_lvl1": {
                    "API": "api2",  # OK, api2 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
                "mod2_lvl1": {
                    "API": "api2",  # OK, api2 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
            },
            "level2": {
                "mod1_lvl2": {
                    "API": "api1",  # OK, api1 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                }
            },
        },
    }

    m = DataSourceModel(**model_dict)

    # when
    models = m.get_models(domain="level1")

    # then
    assert len(models) == 2
    assert "mod1_lvl1" in models
    assert "mod2_lvl1" in models
    assert all([isinstance(v, DomainModel) for v in models.values()])


def test_get_domain_by_model():
    # given

    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
            "api2": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
            },
        },
        "domains": {
            "level1": {
                "mod1_lvl1": {
                    "API": "api2",  # OK, api2 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
                "mod2_lvl1": {
                    "API": "api2",  # OK, api2 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                },
            },
            "level2": {
                "mod1_lvl2": {
                    "API": "api1",  # OK, api1 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                }
            },
        },
    }

    m = DataSourceModel(**model_dict)
    model = m.get_models(domain="level1")["mod1_lvl1"]

    # when
    domain = m.get_domain_name(model)

    # then
    assert domain == "level1"


def test_DataSourceModel_model_headers_override_the_api_ones():
    """case where the model has headers that are merged with the API default headers"""

    # given

    model_dict = {
        "APIs": {
            "api1": {
                "name": "INSEE.Metadonnees",
                "base_url": "https://api.insee.fr/",
                "default_headers": {"accept": "application/xml"},  # default headers
            },
        },
        "domains": {
            "level1": {
                "mod1_lvl1": {
                    "API": "api1",  # OK, api1 is defined
                    "description": "Référentiel géographique INSEE - niveau régional",
                    "type": "JsonExtractor",
                    "endpoint": "/geo/regions",
                    "headers": {"accept": "text/csv", "another_key": "another_value"},
                },
            }
        },
    }

    m = DataSourceModel(**model_dict)

    # when
    model = m.get_models(domain="level1")["mod1_lvl1"]

    # then
    assert model.headers.accept == "text/csv"
    assert model.headers.another_key == "another_value"


def test_get_api():

    # given
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

    model = DomainModel(**model_dict["domains"]["level1"]["mod1_lvl1"])

    # when
    api = m.get_api(model)

    # then
    assert api.name == "INSEE.Metadonnees"


def test_model_name_is_set():
    # given

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

    # when
    m = DataSourceModel(**model_dict)

    # then
    assert m.get_models()["level1.mod1_lvl1"].name == "level1.mod1_lvl1"


def test_table_name_is_derived_from_model_name():
    # given

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

    # when
    m = DataSourceModel(**model_dict)

    # then
    assert m.get_models()["level1.mod1_lvl1"].table_name == "level1_mod1_lvl1"


def test_get_model_nominal():
    # given

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

    # when
    model = m.get_model("level1.mod1_lvl1")

    # then
    assert model.name == "level1.mod1_lvl1"


def test_get_model_raises_ValueError():
    # given

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

    # when
    with pytest.raises(ValueError) as e:
        m.get_model("level1.mod1_lvl2")

    # then
    assert "level1.mod1_lvl2" in str(e.value)


def test_AcceptHeader_simple():
    # given

    headers = {
        "accept": "application/json",
    }

    # when
    model = HeaderModel(**headers)

    # then
    assert model.accept == headers["accept"]


def test_AcceptHeader_zip():
    # given

    headers = {
        "accept": "application/zip",
    }

    # when
    model = HeaderModel(**headers)

    # then
    assert model.accept == headers["accept"]


def test_AcceptHeader_multiple():
    # given

    headers = {
        "accept": "application/zip, application/octet-stream, */*",
    }

    # when
    model = HeaderModel(**headers)

    # then
    assert model.accept == headers["accept"]


def test_AcceptHeader_multiple_space_is_optional():
    # given

    headers = {
        "accept": "application/json,application/xml, */*",
    }

    # when
    model = HeaderModel(**headers)

    # then
    assert model.accept == headers["accept"]


def test_AcceptHeader_incorrect():
    # given

    headers = {
        "accept": "blah, application/xml, */*",
    }

    # when
    with pytest.raises(ValidationError) as e:
        HeaderModel(**headers)

    # then
    assert "accept" in str(e.value)


def test_DataLoadParameters_nominal():
    # given
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
                    "load_params": {
                        "param1": "value1",
                        "param2": 2,
                    },
                },
            }
        },
    }
    m = DataSourceModel(**model_dict)

    # when
    model = m.get_model("level1.mod1_lvl1")

    # then
    assert model.load_params.model_dump()["param1"] == "value1"
    assert model.load_params.model_dump()["param2"] == 2


def test_DataLoadParameters_empty():
    # given
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

    # when
    model = m.get_model("level1.mod1_lvl1")

    # then
    assert model.load_params.model_dump(mode="json") == {
        "separator": ";",
        "header": 0,
        "skipfooter": 0,
    }


def test_DataLoadParameters_default_values():
    # given
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
                    "load_params": {},
                },
            }
        },
    }
    m = DataSourceModel(**model_dict)

    # when
    model = m.get_model("level1.mod1_lvl1")

    # then
    assert model.load_params.model_dump(mode="json") == {
        "separator": ";",
        "header": 0,
        "skipfooter": 0,
    }
