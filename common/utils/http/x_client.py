import logging

import httpx
# from tenacity import (
#     before_log,
#     retry,
#     retry_if_exception_type,
#     stop_after_attempt,
#     stop_after_delay,
# )

from common.utils.interfaces.http import HttpClient, HttpException
from common.utils.logging_odis import logger


class HttpxClient(HttpClient):
    """
    HTTP client based on httpx
    """

    _session: httpx.Client

    def __init__(self, max_connections: int = 100):
        """
        Args:
            max_connections (int, optional): The max number of concurrent connections.
                Defaults to 100.
        """

        self._session = httpx.Client()

    def get(
        self,
        url: str,
        params: dict = None,
        headers: dict = None,
        as_json: bool = False,
    ) -> dict | bytes:
        """
        Send a GET request.

        Args:
            url (str): The URL to send the request to.
            params (dict, optional): Query parameters to include in the request.
            headers (dict, optional): Headers to include in the request.
            as_json (bool, optional): Whether to parse the response as JSON.

        Returns:
            dict: The response data if as_json is True, otherwise the raw response text.
        """

        # # fix on booleans values for params:
        # # see https://github.com/aio-libs/aiohttp/issues/4874
        # params = {
        #     k: str(v).lower() if isinstance(v, bool) else v
        #     for k, v in (params or {}).items()
        # }

        response = self._session.get(url, params=params, headers=headers)
        response.raise_for_status()

        try:
            return response.json() if as_json else response.text
        except httpx.HTTPError as e:
            logger.error(f"Failed to parse response: {e.message}")
            raise HttpException(f"Failed to parse response from {url}: {e}") from e
