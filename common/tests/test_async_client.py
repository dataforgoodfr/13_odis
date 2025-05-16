import asyncio
import re

import aiohttp
import nest_asyncio
import pytest
from aioresponses import aioresponses

from common.utils.http.async_client import AsyncHttpClient
from common.utils.interfaces.http import HttpException

# fix on https://github.com/pytest-dev/pytest-asyncio/issues/112
nest_asyncio.apply()


async def test_async_http_client_as_json():
    # given
    loop = asyncio.get_event_loop()
    url = "http://test.example.com"
    params = {"param1": "value1", "param2": "value2"}
    headers = {"Authorization": "Bearer token"}
    expected_response = {"key": "value"}
    client = AsyncHttpClient()

    with aioresponses() as m:
        pattern = re.compile(r"^http://test\.example\.com/\?.*$")
        m.get(pattern, status=200, payload=expected_response)

        # when
        resp = loop.run_until_complete(
            client.get(url, params=params, headers=headers, as_json=True)
        )

    # then
    m.assert_called_once_with(
        "http://test.example.com", params=params, headers=headers, allow_redirects=True
    )
    assert resp == expected_response


async def test_async_http_client_as_text():
    # given
    loop = asyncio.get_event_loop()
    url = "http://test.example.com"
    params = {"param1": "value1", "param2": "value2"}

    expected_response = "this is a test response"
    client = AsyncHttpClient()

    with aioresponses() as m:
        pattern = re.compile(r"^http://test\.example\.com/\?.*$")
        m.get(pattern, status=200, payload=expected_response)

        # when
        resp = loop.run_until_complete(
            client.get(url, params=params, as_json=False)  # returns text
        )

    # then
    m.assert_called_once_with(
        "http://test.example.com", params=params, allow_redirects=True, headers=None
    )
    assert (
        isinstance(resp,bytes)
    )  # surrounded by quotes, that's why we use "in" instead of "=="


async def test_async_http_client_raises():
    """when the content cannot be parsed as JSON, the exception is raised"""

    # given
    loop = asyncio.get_event_loop()
    url = "http://test.example.com"
    params = {"param1": "value1", "param2": "value2"}

    expected_response = "this is a test response"
    client = AsyncHttpClient()

    with aioresponses() as m:
        pattern = re.compile(r"^http://test\.example\.com/\?.*$")
        m.get(
            pattern, status=200, payload=expected_response, content_type="text/plain"
        )  # simulate a non-JSON response

        # when
        with pytest.raises(HttpException) as excinfo:

            loop.run_until_complete(
                client.get(
                    url,
                    params=params,
                    as_json=True,  # try to parse as JSON should raise an exception
                )
            )

    # then
    assert "Failed to parse response" in str(excinfo.value)


async def test_async_http_client_is_retried_2_times():
    # given
    loop = asyncio.get_event_loop()
    url = "http://test.example.com"
    params = {"param1": "value1", "param2": "value2"}
    headers = {"Authorization": "Bearer token"}
    expected_response = {"key": "value"}
    client = AsyncHttpClient()

    with aioresponses() as m:
        pattern = re.compile(r"^http://test\.example\.com/\?.*$")
        m.get(pattern, status=500)  # simulate a server error
        m.get(pattern, status=500)  # simulate a server error
        m.get(pattern, status=200, payload=expected_response)  # simulate a success

        # when
        resp = loop.run_until_complete(
            client.get(url, params=params, headers=headers, as_json=True)
        )

    # then

    assert resp == expected_response


async def test_async_http_client_fails_on_3_trial():
    # given
    loop = asyncio.get_event_loop()
    url = "http://test.example.com"
    params = {"param1": "value1", "param2": "value2"}
    headers = {"Authorization": "Bearer token"}
    expected_response = {"key": "value"}
    client = AsyncHttpClient()

    with aioresponses() as m:
        pattern = re.compile(r"^http://test\.example\.com/\?.*$")
        m.get(pattern, status=500)  # simulate a server error
        m.get(pattern, status=500)  # simulate a server error
        m.get(pattern, status=500)  # simulate a server error
        m.get(
            pattern, status=200, payload=expected_response
        )  # simulate a success which should NOT be reached

        # when
        with pytest.raises(aiohttp.ClientResponseError) as excinfo:
            loop.run_until_complete(
                client.get(url, params=params, headers=headers, as_json=True)
            )

    # then

    assert "Internal Server Error" in str(excinfo.value)


async def test_async_http_client_bool_params_are_correctly_passed():
    """fixes an issue with boolean values in params which should be passed as
    strings"""

    # given
    loop = asyncio.get_event_loop()
    url = "http://test.example.com"
    params = {"param1": True, "param2": False}
    headers = {"Authorization": "Bearer token"}
    expected_response = {"key": "value"}
    client = AsyncHttpClient()

    with aioresponses() as m:
        pattern = re.compile(r"^http://test\.example\.com/\?.*$")
        m.get(pattern, status=200, payload=expected_response)  # simulate a success

        # when
        resp = loop.run_until_complete(
            client.get(url, params=params, headers=headers, as_json=True)
        )

    # then
    m.assert_called_once_with(
        "http://test.example.com",
        params={"param1": "true", "param2": "false"},
        headers=headers,
        allow_redirects=True,
    )
    assert resp == expected_response
