# src/pipeline/load_service.py

from typing import List
from common.data_source_model import DataSourceModel, DomainModel
from common.utils.factory.loader_factory import create_loader
from common.utils.file_handler import FileHandler
from common.utils.logging_odis import logger


def run_load(config_model: DataSourceModel, data_sources: List[DomainModel]):
    """
    Pure business logic for loading JSON files into PostgreSQL.
    Used by both CLI and Prefect.
    """

    for ds in data_sources:
        logger.info(f"[load] loading {ds.name}")
        loader = create_loader(config_model, ds, handler=FileHandler())
        loader.execute()
        logger.info(f"[load] loaded {ds.name}")
