from dagster import load_assets_from_modules

from . import raw_sales_file
from . import bronze_cdc_customers

all_assets = load_assets_from_modules ([
    raw_sales_file,
    bronze_cdc_customers
])