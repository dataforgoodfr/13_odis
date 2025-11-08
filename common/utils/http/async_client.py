import logging

import aiohttp
from tenacity import (
    before_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    stop_after_delay,
)

from common.utils.interfaces.http import HttpClient, HttpException
from common.utils.logging_odis import logger


class AsyncHttpClient(HttpClient):
    """
    Asynchronous HTTP client for making non-blocking HTTP requests.
    """

    _session: aiohttp.ClientSession
    _timeout: aiohttp.ClientTimeout

    def __init__(self, max_connections: int = 100, timeout: int = 1200):
        """
        Args:
            max_connections (int, optional): The max number of concurrent connections.
                Defaults to 100.
            timeout (int, optional): The timeout in seconds for each individual HTTP request.
                Defaults to 1200 (20 minutes).
        """

        conn = aiohttp.TCPConnector(limit=max_connections)
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session = aiohttp.ClientSession(connector=conn, timeout=self._timeout)

        logger.debug(
            f"AsyncHttpClient initialized with max_connections={max_connections}, timeout={timeout}s"
        )

    @retry(
        retry=retry_if_exception_type(aiohttp.ClientError),
        stop=(stop_after_delay(2400) | stop_after_attempt(3)),
        before=before_log(logger, logging.DEBUG),
        reraise=True,  
    )
    async def get(
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

        # fix on booleans values for params:
        # see https://github.com/aio-libs/aiohttp/issues/4874
        params = {
            k: str(v).lower() if isinstance(v, bool) else v
            for k, v in (params or {}).items()
        }

        async with self._session.get(url, params=params, headers=headers) as response:
            response.raise_for_status()

            try:
                return await response.json() if as_json else await response.read()
            except aiohttp.ContentTypeError as e:
                logger.error(f"Failed to parse response: {e}")
                raise HttpException(f"Failed to parse response from {url}: {e}") from e
