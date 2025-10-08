# sensors.py
from dagster import sensor, RunRequest
import boto3

from pipeline.jobs.raw_sales_job import raw_sales_job

S3_LANDING = "landing-zone"
AWS_ENDPOINT_URL = "http://minio:9000"
AWS_ACCESS_KEY_ID = "minio"
AWS_SECRET_ACCESS_KEY = "minio123"

@sensor(job=raw_sales_job, minimum_interval_seconds=5)
def new_file_sensor(context):
    s3 = boto3.resource(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    bucket = s3.Bucket(S3_LANDING)
    files = list(bucket.objects.all())
    if not files:
        return

    latest_file = max(files, key=lambda x: x.last_modified)
    s3_key = latest_file.key

    last_processed = context.cursor
    if last_processed != s3_key:
        context.update_cursor(s3_key)
        yield RunRequest(run_key=s3_key)
