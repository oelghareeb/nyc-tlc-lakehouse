-- models/silver/yellow_tripdata_clean.sql

with bronze as (
    select *
    from "iceberg"."bronze"."yellow_tripdata_raw"
)

select
    *
from bronze
where total_amount > 0