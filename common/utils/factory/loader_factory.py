from importlib import import_module

from dotenv import dotenv_values

from common.data_source_model import DataSourceModel, DomainModel
from common.utils.database_client import DatabaseClient
from common.utils.interfaces.data_handler import IDataHandler
from common.utils.interfaces.loader import AbstractDataLoader


def create_loader(
    config: DataSourceModel, model: DomainModel, handler: IDataHandler = None
) -> AbstractDataLoader:
    """instanciates the correct extractor class for the given model
    based on the model format

    Args:
        config (DataSourceModel): the config object
        model (DomainModel): the model object
        handler (IDataHandler, optional): the data handler. Defaults to None.

    Example:

    ```python
        # imagine a config object with a model called "my_model"
        config = DataSourceModel(
            domains={
                "my_domain": DomainModel(
                    name="my_model",
                    format="csv",
                    # other attributes...
                )
            }
        )
        model = config.get_models()["my_model"]
        handler = FileHandler()
        loader = create_loader(config, model, handler)
        # loader will be an instance of CSVDataLoader
        # because the format is "csv"
    ```

    Returns:
        AbstractDataLoader: the loader object
    Raises:
        ValueError: if the loader class cannot be found or instantiated
    """

    try:

        loader_name = f"{str.capitalize(model.format)}DataLoader"

        settings = dotenv_values()

        db_client = DatabaseClient(
            settings=settings,
            autocommit=False,
        )

        source_module = import_module(f"common.utils.loader.{model.format}_loader")
        _class: AbstractDataLoader = getattr(source_module, loader_name)

        return _class(config, model, db_client, handler=handler)

    except Exception as e:
        raise ValueError(f"Error creating loader: {str(e)}") from e
