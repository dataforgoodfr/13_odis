from common.data_source_model import DomainModel
from common.utils.interfaces.data_handler import (
    ArtifactLog,
    IDataHandler,
    MetadataInfo,
    PageLog,
    StorageInfo,
)


class StubDataHandler(IDataHandler):
    """used to test the JsonExtractor class"""

    model: DomainModel

    def __init__(self, model: DomainModel):
        self.model = model
        self.is_handled: bool = False

    def file_dump(self, *args, **kwargs) -> StorageInfo:

        return StorageInfo(
            location="test", format="test", file_name="test", encoding="test"
        )

    def dump_metadata(
        self,
        *args,
        **kwargs,
    ):

        self.is_handled = True

        return MetadataInfo(
            **{
                "domain": "test_domain",
                "source": "test_source",
                "operation": "extract",
                "last_run_time": "2023-10-01T00:00:00Z",
                "last_processed_page": 1,
                "complete": True,
                "errors": 0,
                "model": self.model,
                "pages": [
                    PageLog(
                        page=1,
                        storage_info=StorageInfo(
                            location="test",
                            format="test",
                            file_name="test",
                            encoding="test",
                        ),
                        is_last=True,
                        success=True,
                    )
                ],
                "artifacts": [
                    ArtifactLog(
                        name="test_artifact",
                        storage_info=StorageInfo(
                            location="test",
                            format="test",
                            file_name="test",
                            encoding="test",
                        ),
                        load_to_bronze=True,
                        success=True,
                    )
                ],
            }
        )
