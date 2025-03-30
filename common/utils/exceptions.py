class InvalidJson(Exception):
    """Exception raised when the JSON file is invalid or not found"""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class InvalidMetadata(Exception):
    """Exception raised when the metadata file is invalid or not found"""

    """Exception raised when the CSV file is invalid or not found"""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class InvalidCSV(Exception):
    """Exception raised when the CSV file is invalid or not found"""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message
