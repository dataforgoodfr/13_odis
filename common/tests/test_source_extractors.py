from common.data_source_model import DataSourceModel

from .stubs.source_extractor_stub import StubExtractor


def test_json_extractor_init():
    """verify that the JsonExtractor class is correctly initialized"""

    # given

    domain_name = "domain1"
    api_name = "api1"
    source_model = "source_model1"

    config = DataSourceModel(
        **{
            "APIs": {
                api_name: {
                    "base_url": "https://api1.com",
                    "name": "api1 name",
                },
            },
            "domains": {
                domain_name: {
                    source_model: {
                        "API": api_name,
                        "endpoint": "/source_model1",
                        "type": "JsonExtractor",  # type of extractor
                    },
                },
            },
        }
    )
    model = list(config.get_models().values())[0]

    extractor = StubExtractor(config, model)

    # when
    # call the next() method to call the generator
    next(extractor.download())

    # then
    assert extractor.is_download
