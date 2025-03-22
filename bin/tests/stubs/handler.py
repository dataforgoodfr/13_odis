from common.utils.interfaces.data_handler import IDataHandler, StorageInfo


class StubDataHandler(IDataHandler):

    is_called: bool = False

    def file_dump(self, *args, **kwargs) -> StorageInfo:

        self.is_called = True
        return StorageInfo(
            location="test", format="json", file_name="test.json", encoding="utf-8"
        )
