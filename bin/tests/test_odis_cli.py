from unittest.mock import AsyncMock, Mock, mock_open, patch

from typer.testing import CliRunner

from bin.odis import app, data_sources_from_domains_str, data_sources_from_str
from bin.tests.stubs.data_handler import StubDataHandler
from bin.tests.stubs.extractor import StubExtractor
from common.config import load_config
from common.data_source_model import DataSourceModel
from common.utils.interfaces.extractor import ExtractionResult

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


def test_extract_by_source():
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

    with patch("builtins.open", mocked_open_function):
        config_model = load_config(
            "test.yaml",  # this is just a dummy name, we are using the mocked_open_function
            response_model=DataSourceModel,
        )
        ds = data_sources_from_str(
            config_model,
            "geographical_references.regions",
        )
        http_client = AsyncMock()
        handler = Mock()

        with patch(
            "bin.odis.create_extractor",
            return_value=StubExtractor(
                return_value=ExtractionResult(
                    success=True,
                    payload={"data": "mocked data"},
                    is_last=True,
                ),
                config=config_model,
                model=ds[0],
                http_client=http_client,
                handler=handler,
            ),
        ) as mock_create_extractor:

            # when
            result = runner.invoke(
                # nevermind the name of the datasource, we are using the patch
                # we just make sure not to load the default config
                app,
                ["extract", "-s", "geographical_references.regions"],
            )

    # then
    # no exception raised but handler is not called
    assert result.exit_code == 0
    assert "All data extracted successfully" in result.stdout
    mock_create_extractor.assert_called_once()


def test_extract_by_domain():
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
                
            departements:
                API: INSEE.Metadonnees
                type: StubExtractor # nevermind the type, we are using the patch
                endpoint: /geo/departements
                description: Référentiel géographique INSEE - niveau départemental

    """
    mocked_open_function = mock_open(read_data=yaml_config)

    with patch("builtins.open", mocked_open_function):
        config_model = load_config(
            "test.yaml",  # this is just a dummy name, we are using the mocked_open_function
            response_model=DataSourceModel,
        )

        http_client = AsyncMock()
        handler = StubDataHandler(
            config_model.get_model("geographical_references.regions")
        )

        with patch(
            "bin.odis.create_extractor",
            return_value=StubExtractor(
                return_value=ExtractionResult(
                    success=True,
                    payload={"data": "mocked data"},
                    is_last=True,
                ),
                config=config_model,
                model=config_model.get_model("geographical_references.regions"),
                http_client=http_client,
                handler=handler,
            ),
        ) as mock_create_extractor:

            # when
            result = runner.invoke(
                # nevermind the name of the datasource, we are using the patch
                # we just make sure not to load the default config
                app,
                ["extract", "-d", "geographical_references"],
            )

    # then
    # no exception raised but handler is not called
    assert result.exit_code == 0
    assert "All data extracted successfully" in result.stdout
    assert mock_create_extractor.call_count == 2  # there are 2 endpoints in the domain


def test_extract_all_domains():
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
                
                
        education:
            moyenne_eleve_commune:
                API: INSEE.Metadonnees
                type: StubExtractor # nevermind the type, we are using the patch
                endpoint: /fr-en-ecoles-effectifs-nb_classes/exports/json
                description: moyenne d'eleve par classe par commune
                
            

    """
    mocked_open_function = mock_open(read_data=yaml_config)

    # when
    with patch("builtins.open", mocked_open_function):
        config_model = load_config(
            "test.yaml",  # this is just a dummy name, we are using the mocked_open_function
            response_model=DataSourceModel,
        )

        http_client = AsyncMock()
        handler = StubDataHandler(
            config_model.get_model("geographical_references.regions")
        )

        with patch(
            "bin.odis.create_extractor",
            return_value=StubExtractor(
                return_value=ExtractionResult(
                    success=True,
                    payload={"data": "mocked data"},
                    is_last=True,
                ),
                config=config_model,
                model=config_model.get_model("geographical_references.regions"),
                http_client=http_client,
                handler=handler,
            ),
        ) as mock_create_extractor:

            # when
            result = runner.invoke(
                # nevermind the name of the datasource, we are using the patch
                # we just make sure not to load the default config
                app,
                ["extract", "-d", "*", "-c", "test.yaml"],
            )

    # then
    # no exception raised but handler is not called
    assert result.exit_code == 0
    assert "All data extracted successfully" in result.stdout
    assert mock_create_extractor.call_count == 2  # one for each domain


def test_load_data_by_source():
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


def test_load_by_domain():
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
                
            departements:
                API: INSEE.Metadonnees
                type: StubExtractor # nevermind the type, we are using the patch
                endpoint: /geo/departements
                description: Référentiel géographique INSEE - niveau départemental

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
            ["load", "-d", "geographical_references", "-c", "test.yaml"],
        )

    # then
    # no exception raised but handler is not called
    assert result.exit_code == 0
    assert "All data loaded successfully" in result.stdout
    assert mock_create_loader.call_count == 2  # one for each domain


def test_load_all_domains():
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
                
                
        education:
            moyenne_eleve_commune:
                API: INSEE.Metadonnees
                type: StubExtractor # nevermind the type, we are using the patch
                endpoint: /fr-en-ecoles-effectifs-nb_classes/exports/json
                description: moyenne d'eleve par classe par commune
                
            

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
            ["load", "-d", "*", "-c", "test.yaml"],
        )

    # then
    # no exception raised but handler is not called
    assert result.exit_code == 0
    assert "All data loaded successfully" in result.stdout
    assert mock_create_loader.call_count == 2  # one for each domain


def test_data_sources_from_str_with_spaces():
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
                description: Référentiel géographique INSEE - niveau régional
                type: MelodiExtractor
                endpoint: /geo/regions
                format: json

            departements:
                API: INSEE.Metadonnees
                description: Référentiel géographique INSEE - niveau départemental
                type: MelodiExtractor
                endpoint: /geo/departements
                format: json

            communes:
                API: INSEE.Metadonnees
                description: Référentiel géographique GEO - niveau commune
                type: JsonExtractor
                endpoint: /communes?fields=code,nom,population,departement,region,centre
                format: json

    """
    mocked_open_function = mock_open(read_data=yaml_config)

    # when
    with patch("builtins.open", mocked_open_function):
        config_model = load_config(
            "test.yaml",  # this is just a dummy name, we are using the mocked_open_function
            response_model=DataSourceModel,
        )
        result = data_sources_from_str(
            config_model,
            "geographical_references.regions, geographical_references.departements",
        )

    # then
    assert len(result) == 2
    assert result[0].name == "geographical_references.regions"
    assert result[1].name == "geographical_references.departements"


def test_data_sources_from_domains_str_nominal():
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
                description: Référentiel géographique INSEE - niveau régional
                type: MelodiExtractor
                endpoint: /geo/regions

            departements:
                API: INSEE.Metadonnees
                description: Référentiel géographique INSEE - niveau départemental
                type: MelodiExtractor
                endpoint: /geo/departements
                

            communes:
                API: INSEE.Metadonnees
                description: Référentiel géographique GEO - niveau commune
                type: JsonExtractor
                endpoint: /communes?fields=code,nom,population,departement,region,centre
                format: json
                
        education:
            moyenne_eleve_commune:
                API: INSEE.Metadonnees
                description: moyenne d'eleve par classe par commune
                type: JsonExtractor
                endpoint: /fr-en-ecoles-effectifs-nb_classes/exports/json
                

            moyenne_eleve_region:
                API: INSEE.Metadonnees
                description: moyenne d'eleve par classe par region academique
                type: JsonExtractor
                endpoint: /fr-en-ecoles-effectifs-nb_classes/exports/json
                

            moyenne_eleve_departement:
                API: INSEE.Metadonnees
                description: moyenne d'eleve par classe par departement
                type: JsonExtractor
                endpoint: /fr-en-ecoles-effectifs-nb_classes/exports/json
                
        services:
            services:
                API: INSEE.Metadonnees
                description: toto
                type: MelodiExtractor
                endpoint: /data/DS_BPE
                
    """
    mocked_open_function = mock_open(read_data=yaml_config)

    # when
    with patch("builtins.open", mocked_open_function):
        config_model = load_config(
            "test.yaml",  # this is just a dummy name, we are using the mocked_open_function
            response_model=DataSourceModel,
        )
        result = data_sources_from_domains_str(
            config_model,
            "education, services",
        )

    # then
    assert len(result) == 4
    assert result[0].name == "education.moyenne_eleve_commune"
    assert result[1].name == "education.moyenne_eleve_region"
    assert result[2].name == "education.moyenne_eleve_departement"
    assert result[3].name == "services.services"


def test_data_sources_from_domains_str_all():
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
                description: Référentiel géographique INSEE - niveau régional
                type: MelodiExtractor
                endpoint: /geo/regions

            departements:
                API: INSEE.Metadonnees
                description: Référentiel géographique INSEE - niveau départemental
                type: MelodiExtractor
                endpoint: /geo/departements
                

            communes:
                API: INSEE.Metadonnees
                description: Référentiel géographique GEO - niveau commune
                type: JsonExtractor
                endpoint: /communes?fields=code,nom,population,departement,region,centre
                format: json
                
        education:
            moyenne_eleve_commune:
                API: INSEE.Metadonnees
                description: moyenne d'eleve par classe par commune
                type: JsonExtractor
                endpoint: /fr-en-ecoles-effectifs-nb_classes/exports/json
                

            moyenne_eleve_region:
                API: INSEE.Metadonnees
                description: moyenne d'eleve par classe par region academique
                type: JsonExtractor
                endpoint: /fr-en-ecoles-effectifs-nb_classes/exports/json
                

            moyenne_eleve_departement:
                API: INSEE.Metadonnees
                description: moyenne d'eleve par classe par departement
                type: JsonExtractor
                endpoint: /fr-en-ecoles-effectifs-nb_classes/exports/json
                
        services:
            services:
                API: INSEE.Metadonnees
                description: toto
                type: MelodiExtractor
                endpoint: /data/DS_BPE
                
    """
    mocked_open_function = mock_open(read_data=yaml_config)

    # when
    with patch("builtins.open", mocked_open_function):
        config_model = load_config(
            "test.yaml",  # this is just a dummy name, we are using the mocked_open_function
            response_model=DataSourceModel,
        )
        result = data_sources_from_domains_str(
            config_model,
            "*",
        )

    # then
    assert len(result) == 7
    assert all(
        source.name
        in [
            "geographical_references.regions",
            "geographical_references.departements",
            "geographical_references.communes",
            "education.moyenne_eleve_commune",
            "education.moyenne_eleve_region",
            "education.moyenne_eleve_departement",
            "services.services",
        ]
        for source in result
    )


def test_data_sources_from_domains_unknow_domain():
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
                description: Référentiel géographique INSEE - niveau régional
                type: MelodiExtractor
                endpoint: /geo/regions

            departements:
                API: INSEE.Metadonnees
                description: Référentiel géographique INSEE - niveau départemental
                type: MelodiExtractor
                endpoint: /geo/departements
                

            communes:
                API: INSEE.Metadonnees
                description: Référentiel géographique GEO - niveau commune
                type: JsonExtractor
                endpoint: /communes?fields=code,nom,population,departement,region,centre
                format: json
                
        education:
            moyenne_eleve_commune:
                API: INSEE.Metadonnees
                description: moyenne d'eleve par classe par commune
                type: JsonExtractor
                endpoint: /fr-en-ecoles-effectifs-nb_classes/exports/json
                

            moyenne_eleve_region:
                API: INSEE.Metadonnees
                description: moyenne d'eleve par classe par region academique
                type: JsonExtractor
                endpoint: /fr-en-ecoles-effectifs-nb_classes/exports/json
                

            moyenne_eleve_departement:
                API: INSEE.Metadonnees
                description: moyenne d'eleve par classe par departement
                type: JsonExtractor
                endpoint: /fr-en-ecoles-effectifs-nb_classes/exports/json
                
        services:
            services:
                API: INSEE.Metadonnees
                description: toto
                type: MelodiExtractor
                endpoint: /data/DS_BPE
                
    """
    mocked_open_function = mock_open(read_data=yaml_config)

    # when
    with patch("builtins.open", mocked_open_function):
        config_model = load_config(
            "test.yaml",  # this is just a dummy name, we are using the mocked_open_function
            response_model=DataSourceModel,
        )
        result = data_sources_from_domains_str(
            config_model,
            "education, blah",
        )

    # then
    assert len(result) == 3
    assert result[0].name == "education.moyenne_eleve_commune"
    assert result[1].name == "education.moyenne_eleve_region"
    assert result[2].name == "education.moyenne_eleve_departement"
