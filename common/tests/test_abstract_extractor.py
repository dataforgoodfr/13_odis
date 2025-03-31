from common.data_source_model import DataSourceModel

from .stubs.data_handler import StubDataHandler
from .stubs.source_extractor_stub import StubExtractor, StubIterationExtractor


def test_execute_writes_data():
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

    conf = DataSourceModel(**model_dict)
    model = conf.domains["level1"]["mod1_lvl1"]

    stub_handler = StubDataHandler()
    extractor = StubExtractor(conf, model, stub_handler)

    # when
    extractor.execute()

    # then
    assert stub_handler.is_handled


# def test_execute_writes_metadata():
#     # given
#     model_dict = {
#         "APIs": {
#             "api1": {
#                 "name": "INSEE.Metadonnees",
#                 "base_url": "https://api.insee.fr/",
#             },
#         },
#         "domains": {
#             "level1": {
#                 "mod1_lvl1": {
#                     "API": "api1",  # OK, api1 is defined
#                     "description": "Référentiel géographique INSEE - niveau régional",
#                     "type": "JsonExtractor",
#                     "endpoint": "/geo/regions",
#                 },
#             }
#         },
#     }

#     conf = DataSourceModel(**model_dict)
#     model = conf.domains["level1"]["mod1_lvl1"]

#     stub_handler = StubDataHandler()
#     extractor = StubExtractor(conf, model, stub_handler)

#     # when
#     extractor.execute()

#     # # then
#     assert stub_metadata_handler.is_handled


def test_execute_iterates_on_download():
    """verify the method iterates on the download generator"""
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

    conf = DataSourceModel(**model_dict)
    model = conf.domains["level1"]["mod1_lvl1"]

    stub_handler = StubDataHandler()
    extractor = StubIterationExtractor(conf, model, stub_handler)

    # when
    extractor.execute()

    # then
    assert extractor.is_download
    assert extractor.iteration_count == extractor.expected_iterations - 1
