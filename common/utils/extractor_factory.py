from importlib import import_module

from common.data_source_model import DataSourceModel, DomainModel
from common.utils.logging_odis import logger
from common.utils.source_extractors import FileExtractor


def create_extractor(config: DataSourceModel, model: DomainModel) -> FileExtractor:
    """instanciates the correct extractor class for the given model"""

    try:
        source_module = import_module("common.utils.source_extractors")
        _class = getattr(source_module, model.type)

        logger.debug(f"Creating extractor for model : {model}")

        _e = _class(config, model)

        return _e
    except Exception as e:
        raise ValueError(f"Error creating extractor: {str(e)}") from e
