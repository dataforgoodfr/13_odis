from importlib import import_module

from common.data_source_model import DataSourceModel, DomainModel
from common.utils.interfaces.extractor import AbstractSourceExtractor
from common.utils.interfaces.data_handler import IDataHandler


def create_extractor(config: DataSourceModel, model: DomainModel, handler: IDataHandler = None) -> AbstractSourceExtractor:
    """instanciates the correct extractor class for the given model"""

    try:
        source_module = import_module("common.utils.source_extractors")
        _class = getattr(source_module, model.type)
        _e = _class(config, model, handler = handler)

        return _e
    except Exception as e:
        raise ValueError(f"Error creating extractor: {str(e)}") from e
