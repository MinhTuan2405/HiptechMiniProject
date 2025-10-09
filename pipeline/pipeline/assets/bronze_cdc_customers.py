from dagster import asset, AssetExecutionContext
from confluent_kafka import Consumer
import pandas as pd
import json
from deltalake import write_deltalake, DeltaTable
import boto3
from datetime import datetime
import io


@asset()
def bronze_cdc_customers(context: AssetExecutionContext):
    KAFKA_BROKER = "kafka:9092"
    KAFKA_TOPIC = "pgserver.sales.customers"
    GROUP_ID = "kafka"

    S3_BRONZE = "bronze-layer"
    AWS_ACCESS_KEY_ID = "minio"
    AWS_SECRET_ACCESS_KEY = "minio123"
    AWS_ENDPOINT_URL = "http://minio:9000"
    AWS_REGION = "us-east-1"
    MINIO_PREFIX = "customers"

    DELTA_PATH = f"s3://{S3_BRONZE}/{MINIO_PREFIX}/"

    # Kafka consumer
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    })

    # MinIO (S3) resource
    s3 = boto3.resource(
        's3',
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=boto3.session.Config(signature_version='s3v4'),
    )


    # customer schema
    customer_schema = [
            'before.customer_id', 'before.first_name', 'before.last_name', 'before.age', 'before.email', 'before.country', 'before.postal_code', 'before.gender',
            'before.phone_number', 'before.membership_status', 'before.join_date', 'before.customer_type', 'before.customer_created_date', 'before.processing_time',

            'after.customer_id', 'after.first_name', 'after.last_name', 'after.age', 'after.email', 'after.country', 'after.postal_code', 'after.gender',
            'after.phone_number', 'after.membership_status', 'after.join_date', 'after.customer_type', 'after.customer_created_date', 'after.processing_time',

            'op', 'ts_ms'
    ]

    consumer.subscribe([KAFKA_TOPIC])
    all_records = []

    poll_timeout = 10
    start_time = datetime.now()

    while (datetime.now() - start_time).total_seconds() < poll_timeout:
        msg = consumer.poll(1.0)
        context.log.info  (str (msg))
        if msg is None:
            continue
        if msg.error():
            context.log.warning(f"Kafka error: {msg.error()}")
            continue

        value = msg.value()
        context.log.info (str (value))
        if value is None:
            context.log.warning("Received tombstone message — skipping.")
            continue

        record = json.loads(value.decode("utf-8"))
        all_records.append(record)
        context.log.info(f"Received message #{len(all_records)}")


    if not all_records:
        context.log.info("No CDC records found.")
        consumer.close()
        return pd.DataFrame()
    
    context.log.info (all_records)
    df = pd.DataFrame(all_records)
    context.log.info (df)

    before_df = pd.json_normalize(df["before"].fillna({}).tolist()).add_prefix("before.")
    after_df = pd.json_normalize(df["after"].fillna({}).tolist()).add_prefix("after.")
    op_df = df[["op", "ts_ms"]] if "op" in df.columns else pd.DataFrame(columns=["op", "ts_ms"])

    full_df = pd.concat([before_df, after_df, op_df], axis=1)


    full_df.columns = [c.strip() for c in full_df.columns]

    for col in customer_schema:
        if col not in full_df.columns:
            full_df[col] = None

    full_df = full_df[customer_schema]
    full_df = full_df.convert_dtypes().where(pd.notnull(full_df), None)

    for col in full_df.columns:
        if pd.api.types.is_bool_dtype(full_df[col]):
            full_df[col] = full_df[col].astype("boolean")  # nullable boolean
        elif pd.api.types.is_integer_dtype(full_df[col]):
            full_df[col] = full_df[col].astype("Int64")    # nullable integer
        elif pd.api.types.is_float_dtype(full_df[col]):
            full_df[col] = full_df[col].astype("Float64")  # nullable float
        else:
            full_df[col] = full_df[col].astype("string")   # nullable string

    full_df = full_df.where(pd.notnull(full_df), None)



    context.log.info (full_df.head ())
    context.log.info(f"Total CDC records: {len(full_df)}")

    try:
        storage_options = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_ENDPOINT_URL": AWS_ENDPOINT_URL,
            "AWS_REGION": AWS_REGION,
            "AWS_ALLOW_HTTP": "true",
        }

        try:
            DeltaTable(DELTA_PATH, storage_options=storage_options)
            mode = "append"
        except Exception:
            mode = "overwrite"

        write_deltalake(DELTA_PATH, full_df, mode=mode, storage_options=storage_options)
        context.log.info(f"Delta table written to {DELTA_PATH}")
    except Exception as e:
        context.log.error(f"Error writing to Delta table: {e}")

    consumer.close()
    return full_df
