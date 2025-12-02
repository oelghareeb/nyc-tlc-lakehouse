CREATE TABLE iceberg.bronze.yellow_tripdata_raw
WITH (
    format='PARQUET',
    external_location='s3a://lakehouse/bronze/yellow_tripdata/'
);