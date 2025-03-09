from common.utils.source_extractors import JsonExtractor


class JSONStubExtractor(JsonExtractor):
    """used to test the JsonExtractor class"""

    is_download: bool = False

    def download(self, domain: str, source_model_name: str):

        self.is_download = True
        return ""
