from dagster import asset, AssetExecutionContext
from confluent_kafka import Consumer
import pandas as pd
import json
from deltalake import write_deltalake
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

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    DELTA_PATH = f"s3://{S3_BRONZE}/{MINIO_PREFIX}/cdc_{timestamp}"

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

    consumer.subscribe([KAFKA_TOPIC])
    all_records = []

    poll_timeout = 10
    start_time = datetime.now()

    while (datetime.now() - start_time).total_seconds() < poll_timeout:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            context.log.warning(f"Kafka error: {msg.error()}")
            continue

        value = msg.value()
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

    df = pd.DataFrame(all_records)
    before_df = pd.json_normalize(df.get("before", [{}])).add_prefix("before.")
    after_df = pd.json_normalize(df.get("after", [{}])).add_prefix("after.")
    op_df = df.get(["op", "ts_ms"], pd.DataFrame(columns=["op", "ts_ms"]))
    full_df = pd.concat([before_df, after_df, op_df], axis=1)

    context.log.info(f"CDC DataFrame preview:\n{full_df.head().to_string()}")
    context.log.info(f"Total CDC records: {len(full_df)}")

    try:
        write_deltalake(
            DELTA_PATH,
            full_df,
            mode="overwrite",
            storage_options={
                "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
                "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
                "AWS_ENDPOINT_URL": AWS_ENDPOINT_URL,
                "AWS_REGION": AWS_REGION,
                "AWS_ALLOW_HTTP": "true",
            },
        )
        context.log.info(f"Delta table written to {DELTA_PATH}")
    except Exception as e:
        context.log.error(f"Error writing to Delta table: {e}")

    try:
        parquet_buffer = io.BytesIO()
        full_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        key = f"{MINIO_PREFIX}/cdc_{timestamp}/cdc_snapshot.parquet"
        s3.Object(S3_BRONZE, key).put(Body=parquet_buffer.getvalue())
        context.log.info(f"Parquet snapshot uploaded to s3://{S3_BRONZE}/{key}")
    except Exception as e:
        context.log.error(f"Error uploading Parquet to MinIO: {e}")

    consumer.close()
    return full_df
