from .stubs.source_extractor_stub import JSONStubExtractor


def test_json_extractor_init_with_dict_config():
    """verify that the JsonExtractor class is correctly initialized"""

    # given
    config = {
        "APIs": {
            "api1": {
                "url": "https://api1.com",
                "headers": {"Content-Type": "application/json"},
            },
        },
        "domains": {
            "domain1": {
                "source_model1": {
                    "api": "api1",
                    "endpoint": "/source_model1",
                    "type": "JsonExtractor",  # type of extractor
                },
            },
        },
    }

    # when
    extractor = JSONStubExtractor(config, "domain1")
    extractor.download("domain1", "source_model1")

    # then
    assert extractor.is_download
