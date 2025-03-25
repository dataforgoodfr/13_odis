from common.utils.interfaces.data_handler import IDataHandler, StorageInfo


class StubDataHandler(IDataHandler):
    """used to test the JsonExtractor class"""

    is_handled: bool = False

    def file_dump(self, *args, **kwargs) -> StorageInfo:

        self.is_handled = True
        return StorageInfo(
            location="test", format="test", file_name="test", encoding="test"
        )
