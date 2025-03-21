from common.utils.interfaces.data_handler import IDataHandler


class StubDataHandler(IDataHandler):

    is_called: bool = False

    def handle(self, *args, **kwargs) -> None:
        self.is_called = True
