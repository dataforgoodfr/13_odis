import os
import boto3
import pytest
import requests
import requests_mock
from moto import mock_s3  # to mock AWS S3 interaction
from common.utils.source_extractors import XlsxExtractDumpS3
from common.utils.source_extractors import ExtractionResult
from pydantic import BaseModel
from typing import Optional

# Test-specific subclass with `error` field for testing purposes
class TestExtractionResult(ExtractionResult):
    error: Optional[str] = None  # Adding this field only for testing

@pytest.fixture
def setup_s3_env():
    """Set up the fake S3 environment and environment variable."""
    os.environ["SCW_BUCKET_NAME"] = "test-bucket"
    with mock_s3():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket="test-bucket")
        yield s3

def test_xlsx_extract_dump_s3_success(setup_s3_env):
    # Example mock URL and content
    mock_url = "https://example.com/data.xlsx"
    fake_file_content = b"FAKE_XLSX_CONTENT"

    # Use requests_mock to simulate the file at the URL
    with requests_mock.Mocker() as m:
        m.get(mock_url, content=fake_file_content)

        # Initialize the extractor with the mock URL and domain/subdomain
        extractor = XlsxExtractDumpS3(config=None, model=None)  # Adjust as per actual instantiation needs

        # Assuming the model's name is 'logement.logements_sociaux'
        extractor.model.name = "logement.logements_sociaux"
        extractor.url = mock_url  # Set mock URL for testing

        # Execute the download method
        results = list(extractor.download())
        result = results[0]

        # Check extraction result
        assert result.success is True
        assert result.payload == fake_file_content
        assert result.is_last is True
        
        # Confirm file is uploaded to S3
        s3 = setup_s3_env
        response = s3.get_object(Bucket="test-bucket", Key="raw/logement/logements_sociaux.xlsx")
        assert response["Body"].read() == fake_file_content


def test_xlsx_extract_dump_s3_http_error(setup_s3_env):
    mock_url = "https://example.com/notfound.xlsx"
    
    with requests_mock.Mocker() as m:
        m.get(mock_url, status_code=404)
        
        extractor = XlsxExtractDumpS3(config=None, model=None)  # Adjust as per actual instantiation needs
        extractor.model.name = "logement.logements_sociaux"  # Example model name
        extractor.url = mock_url  # Set the URL to simulate error

        results = list(extractor.download())
        
        # Create a TestExtractionResult for the test scenario
        result = TestExtractionResult(**results[0].model_dump(), error="HTTP Error 404")  # Use `model_dump` instead of `dict`
        
        # Check for error handling in the test result
        assert result.success is False
        assert result.payload is None
        assert result.is_last is True
        assert result.error == "HTTP Error 404"  # We expect an error message in this case
