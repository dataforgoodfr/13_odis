import boto3
from moto import mock_aws

from ..utils.storage.object_storage_client import ObjectStorageClient


@mock_aws
def test_mock_aws():
    conn = boto3.resource("s3", region_name="us-east-1")
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    conn.create_bucket(Bucket="mybucket")

    body = conn.Object("mybucket", "steve").get()["Body"].read().decode("utf-8")
    assert True
