-- models/gold/fct_trips.sql

select
    trip_id,

    -- Dimensions
    vendorid as vendor_id,
    ratecodeid as rate_code_id,
    payment_type as payment_type_id,

    -- Surrogate date keys
    pickup_date_id,
    dropoff_date_id,

    -- Surrogate time keys
    pickup_time_id,
    dropoff_time_id,

    -- Raw timestamps
    tpep_pickup_datetime,
    tpep_dropoff_datetime,

    -- Measures
    passenger_count,
    trip_distance,
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
    store_and_fwd_flag

from "iceberg"."nyc_bronze"."yellow_tripdata_raw"