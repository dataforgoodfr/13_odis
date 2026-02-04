import re

import aiohttp
import nest_asyncio
import pytest
from aioresponses import aioresponses

from common.utils.http.async_client import AsyncHttpClient
from common.utils.interfaces.http import HttpException

# fix on https://github.com/pytest-dev/pytest-asyncio/issues/112
nest_asyncio.apply()


@pytest.mark.asyncio
async def test_async_http_client_as_json():
    url = "http://test.example.com"
    params = {"param1": "value1", "param2": "value2"}
    headers = {"Authorization": "Bearer token"}
    expected_response = {"key": "value"}

    async with AsyncHttpClient() as client:
        with aioresponses() as m:
            pattern = re.compile(r"^http://test\.example\.com/\?.*$")
            m.get(pattern, status=200, payload=expected_response)

            resp = await client.get(url, params=params, headers=headers, as_json=True)

    assert resp == expected_response


@pytest.mark.asyncio
async def test_async_http_client_as_text():
    url = "http://test.example.com"
    params = {"param1": "value1", "param2": "value2"}
    expected_response = "this is a test response"

    async with AsyncHttpClient() as client:
        with aioresponses() as m:
            pattern = re.compile(r"^http://test\.example\.com/\?.*$")
            m.get(pattern, status=200, payload=expected_response)

            resp = await client.get(url, params=params, as_json=False)

    assert isinstance(resp, bytes)


@pytest.mark.asyncio
async def test_async_http_client_raises():
    """when the content cannot be parsed as JSON, the exception is raised"""
    url = "http://test.example.com"
    params = {"param1": "value1", "param2": "value2"}
    expected_response = "this is a test response"

    async with AsyncHttpClient() as client:
        with aioresponses() as m:
            pattern = re.compile(r"^http://test\.example\.com/\?.*$")
            m.get(pattern, status=200, payload=expected_response, content_type="text/plain")

            with pytest.raises(HttpException) as excinfo:
                await client.get(url, params=params, as_json=True)

    assert "Failed to parse response" in str(excinfo.value)


@pytest.mark.asyncio
async def test_async_http_client_is_retried_2_times():
    url = "http://test.example.com"
    params = {"param1": "value1", "param2": "value2"}
    headers = {"Authorization": "Bearer token"}
    expected_response = {"key": "value"}

    async with AsyncHttpClient() as client:
        with aioresponses() as m:
            pattern = re.compile(r"^http://test\.example\.com/\?.*$")
            m.get(pattern, status=500)
            m.get(pattern, status=500)
            m.get(pattern, status=200, payload=expected_response)

            resp = await client.get(url, params=params, headers=headers, as_json=True)

    assert resp == expected_response


@pytest.mark.asyncio
async def test_async_http_client_fails_on_3_trial():
    url = "http://test.example.com"
    params = {"param1": "value1", "param2": "value2"}
    headers = {"Authorization": "Bearer token"}

    async with AsyncHttpClient() as client:
        with aioresponses() as m:
            pattern = re.compile(r"^http://test\.example\.com/\?.*$")
            m.get(pattern, status=500)
            m.get(pattern, status=500)
            m.get(pattern, status=500)
            m.get(pattern, status=200, payload={"key": "value"})

            with pytest.raises(aiohttp.ClientResponseError) as excinfo:
                await client.get(url, params=params, headers=headers, as_json=True)

    assert "Internal Server Error" in str(excinfo.value)


@pytest.mark.asyncio
async def test_async_http_client_bool_params_are_correctly_passed():
    """fixes an issue with boolean values in params which should be passed as strings"""
    url = "http://test.example.com"
    params = {"param1": True, "param2": False}
    headers = {"Authorization": "Bearer token"}
    expected_response = {"key": "value"}

    async with AsyncHttpClient() as client:
        with aioresponses() as m:
            pattern = re.compile(r"^http://test\.example\.com/\?.*$")
            m.get(pattern, status=200, payload=expected_response)

            resp = await client.get(url, params=params, headers=headers, as_json=True)

    m.assert_called_once_with(
        "http://test.example.com",
        params={"param1": "true", "param2": "false"},
        headers=headers,
        allow_redirects=True,
    )
    assert resp == expected_response
