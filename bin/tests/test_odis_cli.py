from unittest.mock import mock_open, patch

from typer.testing import CliRunner

from bin.odis import app

runner = CliRunner()


def test_explain_overview():
    """just validate there is no error"""

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
        result = runner.invoke(app, ["explain"])

    # then
    # no exception raised
    assert result.exit_code == 0
    assert "geographical_references" in result.stdout


def test_explain_source_apis():
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
        result = runner.invoke(app, ["explain", "-a", "INSEE.Metadonnees"])

    # then
    # no exception raised
    assert result.exit_code == 0


def test_explain_source_models():
    """verify there is no error when explaining models"""
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
        result = runner.invoke(
            app, ["explain", "-s", "geographical_references.regions"]
        )

    # then
    # no exception raised
    assert result.exit_code == 0


def test_extract_data():
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
                type: StubExtractor # nevermind the type, we are using the patch
                endpoint: /geo/regions
                description: Référentiel géographique INSEE - niveau régional

    """
    mocked_open_function = mock_open(read_data=yaml_config)

    # when
    with patch("builtins.open", mocked_open_function), patch(
        "bin.odis.create_extractor"
    ) as mock_create_extractor:

        # when
        result = runner.invoke(
            # nevermind the name of the datasource, we are using the patch
            # we just make sure not to load the default config
            app,
            ["extract", "-s", "geographical_references.regions", "-c", "test.yaml"],
        )

    # then
    # no exception raised but handler is not called
    assert result.exit_code == 0
    assert "All data extracted successfully" in result.stdout
    mock_create_extractor.assert_called_once()


def test_load_data():
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
                type: StubExtractor # nevermind the type, we are using the patch
                endpoint: /geo/regions
                description: Référentiel géographique INSEE - niveau régional

    """
    mocked_open_function = mock_open(read_data=yaml_config)

    # when
    with patch("builtins.open", mocked_open_function), patch(
        "bin.odis.create_loader"
    ) as mock_create_loader:

        # when
        result = runner.invoke(
            # nevermind the name of the datasource, we are using the patch
            # we just make sure not to load the default config
            app,
            ["load", "-s", "geographical_references.regions", "-c", "test.yaml"],
        )

    # then
    # no exception raised but handler is not called
    assert result.exit_code == 0
    assert "All data loaded successfully" in result.stdout
    mock_create_loader.assert_called_once()
