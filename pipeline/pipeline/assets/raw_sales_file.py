from dagster import asset
import pandas as pd
from io import BytesIO
from deltalake import write_deltalake
import boto3
import os
import tempfile

@asset(name="raw_sales_file")
def raw_sales_file():
    # MinIO config
    S3_LANDING = "landing-zone"
    S3_BRONZE = "bronze-layer"
    
    AWS_ACCESS_KEY_ID = "minio"
    AWS_SECRET_ACCESS_KEY = "minio123"
    AWS_ENDPOINT_URL = "http://minio:9000"
    AWS_REGION = "us-east-1"

    s3 = boto3.resource(
        's3',
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=boto3.session.Config(signature_version='s3v4'),
    )

    bucket = s3.Bucket(S3_LANDING)
    files = list(bucket.objects.all())
    if not files:
        print("No files found in landing-zone")
        return

    latest_file = max(files, key=lambda x: x.last_modified)
    key = latest_file.key
    print(f"Processing file: {key}")

    obj = s3.Object(S3_LANDING, key)
    content = obj.get()['Body'].read()

    if key.endswith(".csv"):
        df = pd.read_csv(BytesIO(content))
    elif key.endswith(".json"):
        df = pd.read_json(BytesIO(content))
    else:
        raise ValueError("Unsupported file format")

    file_base_name = os.path.splitext(os.path.basename(key))[0]

    with tempfile.TemporaryDirectory() as tmpdir:
        local_delta_path = os.path.join(tmpdir, file_base_name)
        write_deltalake(local_delta_path, df, mode="overwrite")

        for root, dirs, files_in_dir in os.walk(local_delta_path):
            for file_name in files_in_dir:
                local_file = os.path.join(root, file_name)
                relative_path = os.path.relpath(local_file, local_delta_path)

                s3_key = f"{file_base_name}/{relative_path}"
                s3.Bucket(S3_BRONZE).upload_file(local_file, s3_key)


    print(f"File {key} processed and uploaded to s3://{S3_BRONZE}/{file_base_name}")
