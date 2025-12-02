-- models/gold/dim_pickup_location.sql
select distinct
    pulocationid as location_id,
    zone,
    borough
from "iceberg"."nyc_silver"."stg_yellow_tripdata"