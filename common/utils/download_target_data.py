from dotenv import load_dotenv
load_dotenv()
import os
from common.utils.object_storage_client import ObjectStorageClient

def download_s3_folder(bucket_name, s3_folder_path):
    s3_client = ObjectStorageClient(bucket_name)
    objects = s3_client.list_objects(s3_folder_path)
    for obj in objects:
        # Skip the folder object itself
        if obj['Key'].endswith('/') or obj['Size'] == 0:
            continue

        filename = os.path.basename(obj['Key'])
        local_path = os.path.join('data/imports/', filename)
        
        s3_client.download_object(obj['Key'], local_path)
        print(f"Downloaded: {obj['Key']} to {local_path}")

if __name__ == '__main__':
    download_s3_folder(os.getenv('SCW_BUCKET_NAME'), 'data_targets/')
