from abc import ABC, abstractmethod

import orjson
from pydantic import ValidationError

from common.data_source_model import DataSourceModel, DomainModel
from common.utils.file_handler import FileHandler
from common.utils.interfaces.data_handler import MetadataInfo
from common.utils.logging_odis import logger


class AbstractDataLoader(ABC):
    """Interface for data loaders."""

    config: DataSourceModel
    model: DomainModel
    handler: FileHandler
    metadata_handler: FileHandler

    def __init__(self, config: DataSourceModel, model: DomainModel):
        self.config = config
        self.model = model

        self.handler = FileHandler()
        self.metadata_handler = FileHandler(file_name=f"{model.name}_extract_log.json")

    @abstractmethod
    def load_data(self):
        pass

    def load_metadata(self) -> MetadataInfo:
        """
        TODO: metadata could be stored in a different location, or in a DB
        """

        metadata_filepath = self.metadata_handler._data_dir(
            self.model
        ) / self.metadata_handler.file_name(self.model)

        try:

            with open(metadata_filepath, "r") as f:
                metadata = orjson.loads(f.read())

            return MetadataInfo(**metadata)

        except orjson.JSONDecodeError as e:
            logger.exception(f"Invalid JSON format in {metadata_filepath}: {str(e)}")

        except ValidationError as e:
            logger.exception(
                f"Invalid metadata format in {metadata_filepath}: {str(e)}"
            )

        except Exception as e:
            logger.exception(f"Error reading file {metadata_filepath}: {str(e)}")

        raise
