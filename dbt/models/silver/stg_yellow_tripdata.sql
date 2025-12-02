-- models/silver/stg_yellow_tripdata.sql

with bronze as (
    select *
    from {{ source('raw', 'yellow_tripdata_raw') }}
)

select
    vendorid,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    pulocationid,
    dolocationid
from bronze
where total_amount > 0
