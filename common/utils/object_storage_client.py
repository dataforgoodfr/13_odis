import os
import boto3
from botocore.client import Config

"""Client class to interact with Scaleway Object Storage."""

class ObjectStorageClient():
    
    def __init__(self, default_bucket_name=None):
        # Need to use V2 signature for upload and V4 for download
        self.client_v2 = self.build_client("s3")
        self.client_v4 = self.build_client("s3v4")
        self.default_bucket_name = default_bucket_name

    @staticmethod
    def build_client(signature_version: str = "s3v4"):
            return boto3.session.Session().client(
                service_name='s3',
                config=Config(signature_version=signature_version),
                region_name=os.getenv('SCW_REGION'),
                use_ssl=True,
                endpoint_url=os.getenv('SCW_OBJECT_STORAGE_ENDPOINT'),
                aws_access_key_id=os.getenv('SCW_ACCESS_KEY'),
                aws_secret_access_key=os.getenv('SCW_SECRET_KEY'),
            )

    def list_buckets(self):
        response = self.client_v4.list_buckets()
        return response['Buckets']

    def list_objects(self, folder='', bucket_name=None):
        bucket_name = bucket_name or self.default_bucket_name
        response = self.client_v4.list_objects(Bucket=bucket_name, Prefix=folder)
        return response['Contents']

    def download_object(self, key, local_path, bucket_name=None):
        bucket_name = bucket_name or self.default_bucket_name
        self.client_v4.download_file(bucket_name, key, local_path)

    def upload_object(self, local_path, object_name=None, bucket_name=None):
        if object_name is None:
            object_name = os.path.basename(local_path)
        bucket_name = bucket_name or self.default_bucket_name
        self.client_v2.upload_file(local_path, bucket_name, object_name)

    def delete_object(self, key, bucket_name=None):
        bucket_name = bucket_name or self.default_bucket_name
        self.client_v4.delete_object(Bucket=bucket_name, Key=key)
