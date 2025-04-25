from typing import Protocol


class HttpClient(Protocol):
    """
    Protocol for HTTP clients.
    """

    def get(
        self, url: str, headers: dict, params: dict, as_json: bool, **kwargs
    ) -> dict | str:
        """
        Send a GET request.

        Args:
            url (str): The URL to send the request to.
            headers (dict): Headers to include in the request.
            params (dict): Query parameters to include in the request.
            as_json (bool): Whether to parse the response as JSON.
            **kwargs: Additional arguments to pass to the request.

        Returns:
            dict | str: The response data if as_json is True, otherwise the raw response text.
        """
        ...
