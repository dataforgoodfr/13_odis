from importlib import import_module

from dotenv import dotenv_values

from common.data_source_model import DataSourceModel, DomainModel
from common.utils.database_client import DatabaseClient
from common.utils.interfaces.loader import AbstractDataLoader


def create_loader(config: DataSourceModel, model: DomainModel) -> AbstractDataLoader:
    """instanciates the correct extractor class for the given model"""

    try:
        loader_name = f"{str.capitalize(model.format)}DataLoader"

        settings = dotenv_values()

        db_client = DatabaseClient(
            settings=settings,
            autocommit=False,
        )

        source_module = import_module("common.utils.data_loaders")
        _class = getattr(source_module, loader_name)
        _e = _class(config, model, db_client)

        return _e
    except Exception as e:
        raise ValueError(f"Error creating extractor: {str(e)}") from e
