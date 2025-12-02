
  
    

    create table "iceberg"."bronze_bronze"."yellow_tripdata_raw"
      
      
    as (
      CREATE TABLE iceberg.bronze.yellow_tripdata_raw
WITH (
    format='PARQUET',
    external_location='s3a://lakehouse/bronze/yellow_tripdata/'
);
    );

  