from dagster import job
from pipeline.assets.raw_sales_file import raw_sales_file
@job
def raw_sales_job():
    raw_sales_file()
