-- models/gold/dim_dropoff_location.sql
select distinct
    dolocationid as location_id,
    zone,
    borough
from "iceberg"."nyc_silver"."stg_yellow_tripdata"