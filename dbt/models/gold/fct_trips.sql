-- models/gold/fct_trips.sql

with trips as (
    select *
    from {{ source('raw', 'yellow_tripdata_raw') }}
),

fact as (
    select
        t.trip_id,

        -- Dimensions
        t.vendorid as vendor_id,
        t.ratecodeid as rate_code_id,
        t.payment_type as payment_type_id,

        -- Surrogate date keys
        d_pickup.date_id as pickup_date_id,
        d_dropoff.date_id as dropoff_date_id,

        -- Surrogate time keys
        tp.pickup_time_id,
        td.dropoff_time_id,

        -- Raw timestamps
        t.tpep_pickup_datetime,
        t.tpep_dropoff_datetime,

        -- Measures
        t.passenger_count,
        t.trip_distance,
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.total_amount,
        t.congestion_surcharge,
        t.airport_fee,
        t.cbd_congestion_fee,
        t.store_and_fwd_flag

    from trips t

    -- Join dim_date for pickup
    left join {{ ref('dim_date') }} d_pickup
        on d_pickup.date_id = t.pickup_date_id

    -- Join dim_date for dropoff
    left join {{ ref('dim_date') }} d_dropoff
        on d_dropoff.date_id = t.dropoff_date_id

    -- Join dim_time for pickup
    left join {{ ref('dim_time') }} tp
        on tp.time_id = t.pickup_time_id

    -- Join dim_time for dropoff
    left join {{ ref('dim_time') }} td
        on td.time_id = t.dropoff_time_id
)

select * from fact
