from pathlib import Path


from common.data_source_model import DataSourceModel
from common.utils.source_extractors import NotebookExtractor

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
                        "description": "Valid test description"
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


def test_NotebookExtractor_with_several_cells():
    """
    each cell stdout is a json object
    """

    # given
    # the notebook should generate data in json format
    config = DataSourceModel(
        **{
            "domains": {
                "my_domain": {
                    "my_notebook": {
                        "type": "NotebookExtractor",
                        "notebook_path": f"{Path.cwd()}/common/tests/notebooks/test_notebook_cells.ipynb",
                        "format": "json",  # format of the result
                    },
                },
            },
        }
    )
    model = list(config.get_models().values())[0]

    extractor = NotebookExtractor(
        config,
        model,
    )

    # when
    # call the next() method to call the generator
    payload_1 = next(extractor.download()).payload

    # 2 cells in the notebook
    assert len(payload_1) == 2
