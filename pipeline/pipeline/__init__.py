from dagster import Definitions, load_assets_from_modules

from pipeline import assets  # noqa: TID252
from pipeline.config.io_manager_config import *
from pipeline.jobs.raw_sales_job import raw_sales_job
from pipeline.sensors.new_file_sensor import new_file_sensor

# from pipeline.resource.minio_client import minio_client_resource

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[raw_sales_job],
    sensors=[new_file_sensor],
    # resources={
    #     "minio_client": minio_client_resource.configured(MINIO_CONFIG)
    # },
)
