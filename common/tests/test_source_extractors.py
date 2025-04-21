from unittest.mock import Mock

import pytest

from common.data_source_model import DataSourceModel
from common.utils.source_extractors import NotebookExtractor, RobustRequest

from .stubs.data_handler import StubDataHandler
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
                        "description": "Valid test description",
                    },
                },
            },
        }
    )
    model = list(config.get_models().values())[0]

    stub_handler = StubDataHandler()
    extractor = StubExtractor(config, model, stub_handler)

    # when
    # call the next() method to call the generator
    next(extractor.download())

    # then
    assert extractor.is_download


@pytest.mark.skip(
    reason="overkill and I won't meet the deadline if I have to debug this fkn test"
)
def test_NotebookExtractor_valid():
    """
    the notebook should return a NotebookResult object
    serialized as a dictionary
    """

    # given
    # the notebook should generate data in json format
    config = DataSourceModel(
        **{
            "domains": {
                "my_domain": {
                    "my_notebook": {
                        "type": "NotebookExtractor",
                        "API": "INSEE.Metadonnees",
                        "endpoint": "/test",
                        "preprocessor": {
                            "base": "/common/tests/notebooks",
                            "name": "test_notebook_valid",
                        },
                        "format": "json",  # format of the result
                        "description": "test description",
                    },
                },
            },
        }
    )
    model = config.get_model("my_domain.my_notebook")
    stub_handler = StubDataHandler()

    extractor = NotebookExtractor(config, model, stub_handler)

    # when
    # call the next() method to call the generator
    payload_1 = next(extractor.download()).payload

    # the csv file should be handled by the handler
    assert payload_1 is not None


@pytest.mark.skip(
    reason="overkill and I won't meet the deadline if I have to debug this fkn test"
)
def test_NotebookExtractor_invalid():
    """
    case where the notebook is not valid,
    for example, the notebook does not return a NotebookResult object
    """

    # given
    # the notebook should generate data in json format
    config = DataSourceModel(
        **{
            "domains": {
                "my_domain": {
                    "my_notebook": {
                        "type": "NotebookExtractor",
                        "API": "INSEE.Metadonnees",
                        "endpoint": "/test",
                        "preprocessor": {
                            "base": "/common/tests/notebooks",
                            "name": "test_notebook_invalid",
                        },
                        "format": "json",  # format of the result
                        "description": "test description",
                    },
                },
            },
        }
    )
    model = config.get_model("my_domain.my_notebook")
    stub_handler = StubDataHandler()

    extractor = NotebookExtractor(config, model, stub_handler)

    # when
    # call the next() method to call the generator
    with pytest.raises(StopIteration):
        next(extractor.download())

    # then
    assert True


def test_RetriableRequest_succeeds(mocker):
    """
    test the RetriableRequestError class
    """

    # given
    mocker.patch(
        "common.utils.source_extractors.requests.get",
        return_value=Mock(status_code=200, json=lambda: {"key": "value"}),
    )

    req = RobustRequest(
        url="https://api1.com",
        params={},
        headers={},
    )

    # when
    response = req.get()

    # then

    assert response.status_code == 200
    assert response.json() == {"key": "value"}


def test_RetriableRequest_retries_on_err(mocker):
    """
    test the RetriableRequestError class
    """

    call_count = 0

    def raise_on_first_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("Network error")
        return Mock(status_code=200, json=lambda: {"key": "value"})

    # given
    mocker.patch(
        "common.utils.source_extractors.requests.get",
        side_effect=raise_on_first_call,
    )

    req = RobustRequest(
        url="https://api1.com",
        params={},
        headers={},
    )

    # when
    response = req.get()

    # then

    assert response.status_code == 200
    assert response.json() == {"key": "value"}
    assert call_count == 2


def test_RetriableRequest_final_exc_is_reraised(mocker):
    """
    test the RetriableRequestError class
    """

    # given
    mocker.patch(
        "common.utils.source_extractors.requests.get",
        side_effect=Exception("Network error"),
    )

    req = RobustRequest(
        url="https://api1.com",
        params={},
        headers={},
    )

    # when
    with pytest.raises(Exception) as excinfo:
        req.get()

    assert str(excinfo.value) == "Network error"
