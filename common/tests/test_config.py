import json
from unittest.mock import mock_open, patch

import pytest

from common.config import load_config
from common.data_source_model import DataSourceModel


def test_load_yaml_config():
    """mock the yaml file and verify that the config is loaded correctly"""

    # given
    yaml_config = """
    APIs:
        INSEE.Metadonnees:
            name: Metadonnees INSEE
            description: INSEE - API des métadonnées
            base_url: https://api.insee.fr/metadonnees/V1
            apidoc: https://api.insee.fr/catalogue/site/themes/wso2/subthemes/insee/pages/item-info.jag?name=M%C3%A9tadonn%C3%A9es&version=V1&provider=insee


    domains:
        geographical_references:
            regions:
                API: INSEE.Metadonnees
                type: JsonExtractor
                endpoint: /geo/regions
                description: Référentiel géographique INSEE - niveau régional

    """
    mocked_open_function = mock_open(read_data=yaml_config)

    # when
    with patch("builtins.open", mocked_open_function):
        result = load_config("")  # any path will do

    # then
    assert result is not None
    assert isinstance(result, dict)
    assert "APIs" in result
    assert "domains" in result
    assert "geographical_references" in result["domains"]
    assert "regions" in result["domains"]["geographical_references"]
    assert "API" in result["domains"]["geographical_references"]["regions"]
    assert "description" in result["domains"]["geographical_references"]["regions"]
    assert "type" in result["domains"]["geographical_references"]["regions"]
    assert "endpoint" in result["domains"]["geographical_references"]["regions"]
    assert (
        result["domains"]["geographical_references"]["regions"]["API"]
        == "INSEE.Metadonnees"
    )
    assert (
        result["domains"]["geographical_references"]["regions"]["description"]
        == "Référentiel géographique INSEE - niveau régional"
    )
    assert (
        result["domains"]["geographical_references"]["regions"]["type"]
        == "JsonExtractor"
    )
    assert (
        result["domains"]["geographical_references"]["regions"]["endpoint"]
        == "/geo/regions"
    )


def test_load_yaml_several_domains_apis():

    # given
    yaml_config = """
    APIs:

        api1:
            name: Metadonnees INSEE
            description: INSEE - API des métadonnées
            base_url: https://api.insee.fr/metadonnees/V1
            apidoc: https://api.insee.fr/catalogue/site/themes/wso2/subthemes/insee/pages/item-info.jag?name=M%C3%A9tadonn%C3%A9es&version=V1&provider=insee
        
        api2:
            name: Metadonnees INSEE
            description: INSEE - API des métadonnées
            base_url: https://api.insee.fr/metadonnees/V1
            apidoc: https://api.insee.fr/catalogue/site/themes/wso2/subthemes/insee/pages/item-info.jag?name=M%C3%A9tadonn%C3%A9es&version=V1&provider=insee
    

    domains:

        domain1:

            regions:
                API: api1
                description: Référentiel géographique INSEE - niveau régional
                type: JsonExtractor
                endpoint: /geo/regions
                
        domain2:

            regions:
                API: api2
                description: Référentiel géographique INSEE - niveau régional
                type: JsonExtractor
                endpoint: /geo/regions

    """
    mocked_open_function = mock_open(read_data=yaml_config)

    # when
    with patch("builtins.open", mocked_open_function):
        result = load_config("")  # any path will do

    # then
    assert result is not None
    assert isinstance(result, dict)
    assert "APIs" in result and len(result["APIs"]) == 2
    assert "domains" in result and len(result["domains"]) == 2


def test_load_invalid_yaml():

    # given
    yaml_config = """blah blah blah"""

    mocked_open_function = mock_open(read_data=yaml_config)

    # when
    # then
    with patch("builtins.open", mocked_open_function), pytest.raises(  # noqa B017
        Exception
    ):
        load_config("")  # any path will do


def test_load_invalid_yaml_structure():
    """case where the yaml file is valid, but does not match the expected structure"""

    # given
    yaml_config = """
    JohnDoe:
        name: John Doe
    """

    mocked_open_function = mock_open(read_data=yaml_config)

    # when
    with patch("builtins.open", mocked_open_function), pytest.raises(  # noqa B017
        Exception
    ) as e:
        load_config("", response_model=DataSourceModel)  # any path will do

    # then

    assert "domains" in str(e.value)


def test_load_config_as_dict_is_json_serializable():

    # given
    yaml_config = """
    APIs:
        INSEE.Metadonnees:
            name: Metadonnees INSEE
            description: INSEE - API des métadonnées
            base_url: https://api.insee.fr/metadonnees/V1
            apidoc: https://api.insee.fr/catalogue/site/themes/wso2/subthemes/insee/pages/item-info.jag?name=M%C3%A9tadonn%C3%A9es&version=V1&provider=insee
            
    domains:
        geographical_references:
            regions:
                API: INSEE.Metadonnees
                type: JsonExtractor
                endpoint: /geo/regions
                description: Référentiel géographique INSEE - niveau régional
    """
    mocked_open_function = mock_open(read_data=yaml_config)

    # when
    with patch("builtins.open", mocked_open_function):
        result = json.dumps(load_config(""))

    # then
    assert result is not None


def test_load_config_as_model():
    # given

    yaml_config = """
    APIs:
        INSEE.Metadonnees:
            name: Metadonnees INSEE
            description: INSEE - API des métadonnées
            base_url: https://api.insee.fr/metadonnees/V1
            apidoc: https://api.insee.fr/catalogue/site/themes/wso2/subthemes/insee/pages/item-info.jag?name=M%C3%A9tadonn%C3%A9es&version=V1&provider=insee
            
    domains:
        geographical_references:
            regions:
                API: INSEE.Metadonnees
                type: JsonExtractor
                endpoint: /geo/regions
                description: Référentiel géographique INSEE - niveau régional
    """

    mocked_open_function = mock_open(read_data=yaml_config)

    # when
    with patch("builtins.open", mocked_open_function):
        result = load_config("", response_model=DataSourceModel)

    # then
    assert result is not None
    assert isinstance(result, DataSourceModel)


def test_load_config_as_model_fails():
    # given

    yaml_config = """
    my_key: my_value
    """

    mocked_open_function = mock_open(read_data=yaml_config)

    # when
    with patch("builtins.open", mocked_open_function), pytest.raises(  # noqa B017
        Exception
    ) as e:
        # try to load a yaml that does not match the model
        load_config("", response_model=DataSourceModel)  # any path will do

    # then

    assert "domains" in str(e.value)


def test_load_config_default_model_is_dict():
    # given
    yaml_config = """
    my_key: my_value
    """

    mocked_open_function = mock_open(read_data=yaml_config)

    # when
    with patch("builtins.open", mocked_open_function):
        result = load_config("")

    # then
    assert result is not None
    assert result == {"my_key": "my_value"}
