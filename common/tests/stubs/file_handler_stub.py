import os

from common.data_source_model import DomainModel
from common.utils.file_handler import FileHandler
from common.utils.interfaces.data_handler import MetadataInfo, OperationType, PageLog, ArtifactLog


class FileHandlerForCSVStub(FileHandler):

    test_data_dir: str
    file_name: str
    page_logs: list[PageLog]
    artifacts: list[ArtifactLog]

    def __init__(
        self,
        test_data_dir: str = None,
        file_name: str = None,
        page_logs: list[PageLog] = None,
        artifact_logs: list[ArtifactLog] = None,
    ):

        if test_data_dir is None:
            test_data_dir = os.path.split(os.path.abspath(__file__))[0] + "/../data"

        if file_name is None:
            file_name = "test_data.csv"

        if page_logs is None or len(page_logs) == 0:
            page_logs = [
                PageLog(
                    page=1,
                    success=True,
                    is_last=True,
                    storage_info={
                        "location": test_data_dir,
                        "format": "csv",
                        "file_name": file_name,
                        "encoding": "utf-8",
                    },
                )
            ]

        if artifact_logs is None or len(artifact_logs) == 0:
            artifact_logs = [
                ArtifactLog(
                    name = "stub_artifact",
                    success=True,
                    load_to_bronze = True,
                    storage_info={
                        "location": test_data_dir,
                        "format": "csv",
                        "file_name": file_name,
                        "encoding": "utf-8",
                    },
                )
            ]

        self.test_data_dir = test_data_dir
        self.file_name = file_name
        self.page_logs = page_logs
        self.artifact_logs = artifact_logs

    def load_metadata(
        self, model: DomainModel, operation: OperationType, *args, **kwargs
    ) -> MetadataInfo:

        return MetadataInfo(
            domain="domain1",
            source="api1",
            operation="load",
            last_run_time="2023-10-01T00:00:00Z",
            last_processed_page=1,
            complete=True,
            errors=0,
            model=model,
            pages = self.page_logs,  # type: ignore[assignment] # TODO: fix this type error
            artifacts = self.artifact_logs
        )


class FileHandlerForJsonStub(FileHandler):

    test_data_dir: str
    file_name: str
    page_logs: list[PageLog]
    artifacts: list[ArtifactLog]

    def __init__(
        self,
        test_data_dir: str = None,
        file_name: str = None,
        page_logs: list[PageLog] = None,
        artifact_logs: list[ArtifactLog] = None,
    ):

        if test_data_dir is None:
            test_data_dir = os.path.split(os.path.abspath(__file__))[0] + "/../data"

        if file_name is None:
            file_name = "test_data.json"

        if page_logs is None or len(page_logs) == 0:
            page_logs = [
                PageLog(
                    page=1,
                    success=True,
                    is_last=True,
                    storage_info={
                        "location": test_data_dir,
                        "format": "json",
                        "file_name": file_name,
                        "encoding": "utf-8",
                    },
                )
            ]
        
        if artifact_logs is None or len(artifact_logs) == 0:
            artifact_logs = [
                ArtifactLog(
                    name = "stub_artifact",
                    success=True,
                    load_to_bronze = True,
                    storage_info={
                        "location": test_data_dir,
                        "format": "csv",
                        "file_name": file_name,
                        "encoding": "utf-8",
                    },
                )
            ]

        self.test_data_dir = test_data_dir
        self.file_name = file_name
        self.page_logs = page_logs
        self.artifact_logs = artifact_logs

    def load_metadata(
        self, model: DomainModel, operation: OperationType, *args, **kwargs
    ) -> MetadataInfo:

        return MetadataInfo(
            domain="domain1",
            source="api1",
            operation="load",
            last_run_time="2023-10-01T00:00:00Z",
            last_processed_page=1,
            complete=True,
            errors=0,
            model=model,
            pages=self.page_logs,  # type: ignore[assignment] # TODO: fix this type error
            artifacts=self.artifact_logs
        )
