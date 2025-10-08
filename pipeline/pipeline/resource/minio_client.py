import boto3
from dagster import resource, Config


class MinIOConfig(Config):
    endpoint_url: str
    aws_access_key_id: str
    aws_secret_access_key: str


@resource(config_schema=MinIOConfig.to_config_schema())
def minio_client_resource(init_context):
    cfg = init_context.resource_config
    client = boto3.client(
        "s3",
        endpoint_url=cfg["endpoint_url"],
        aws_access_key_id=cfg["aws_access_key_id"],
        aws_secret_access_key=cfg["aws_secret_access_key"]
    )
    init_context.log.info(f"##### Connect MinIO: {cfg['endpoint_url']}")
    return client
