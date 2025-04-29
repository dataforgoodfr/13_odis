from importlib import import_module

from common.data_source_model import DataSourceModel, DomainModel
from common.utils.interfaces.data_handler import IDataHandler
from common.utils.interfaces.extractor import AbstractSourceExtractor
from common.utils.interfaces.http import HttpClient


def create_extractor(
    config: DataSourceModel,
    model: DomainModel,
    http_client: HttpClient,
    handler: IDataHandler = None,
) -> AbstractSourceExtractor:
    """instanciates the correct extractor class for the given model"""

    try:
        source_module = import_module("common.utils.source_extractors")
        _class: AbstractSourceExtractor = getattr(source_module, model.type)
        _e = _class(config, model, http_client, handler=handler)

        return _e
    except Exception as e:
        raise ValueError(f"Error creating extractor: {str(e)}") from e
