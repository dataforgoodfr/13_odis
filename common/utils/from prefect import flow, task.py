import time

from prefect import flow, task

from common.data_source_model import DataSourceModel, DomainModel
from common.utils.interfaces.data_handler import DataArtifact, ArtifactLog

from common.utils.factory.extractor_factory import create_extractor
from common.utils.factory.loader_factory import create_loader

@flow
def extract_and_load()
