from .stubs.source_extractor_stub import JSONStubExtractor


def test_json_extractor_init():
    """verify that the JsonExtractor class is correctly initialized"""

    # given

    domain_name = "domain1"
    api_name = "api1"
    source_model = "source_model1"

    config = {
        "APIs": {
            api_name: {
                "base_url": "https://api1.com",
                "headers": {"Content-Type": "application/json"},
            },
        },
        "domains": {
            domain_name: {
                source_model: {
                    "api": api_name,
                    "endpoint": "/source_model1",
                    "type": "JsonExtractor",  # type of extractor
                },
            },
        },
    }

    extractor = JSONStubExtractor(config, domain_name)

    # when
    extractor.download(domain_name, source_model)

    # then
    assert extractor.is_download

    # check self.api_confs
    assert extractor.api_confs == config["APIs"]

    # check self.source_models
    assert extractor.source_models == config["domains"][domain_name]


def test_set_query_parameters():

    # given
    domain_name = "domain1"
    api_name = "api1"
    source_model = "source_model1"

    config = {
        "APIs": {
            api_name: {
                "base_url": "https://api1.com",
                "default_headers": {"accept": "application/json"},
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

    extractor = JSONStubExtractor(config, domain_name)

    # when
    extractor.set_query_parameters(source_model)

    # then
    assert extractor.url == "https://api1.com/source_model1"
    assert extractor.headers == config["APIs"][api_name]["default_headers"]
    assert extractor.params is None
    assert extractor.format == "json"


def test_set_query_parameters_with_params():
    pass


def test_set_query_parameters_with_format():
    pass
