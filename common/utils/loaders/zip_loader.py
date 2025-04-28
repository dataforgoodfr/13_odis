from common.utils.loaders.notebook_loader import NotebookLoader


class ZipDataLoader(NotebookLoader):

    columns: list[str] = []
    format = "zip"
