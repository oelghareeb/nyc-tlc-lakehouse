-- models/gold/dim_time.sql
select distinct
    hour(tpep_pickup_datetime)*3600 + minute(tpep_pickup_datetime)*60 + second(tpep_pickup_datetime) as time_id,
    hour(tpep_pickup_datetime) as hour,
    minute(tpep_pickup_datetime) as minute,
    second(tpep_pickup_datetime) as second,
case when hour(tpep_pickup_datetime) between 6 and 9   -- 6:00 to 9:59 AM
     or hour(tpep_pickup_datetime) between 16 and 19 then true  -- 4:00 to 7:59 PM
     else false end as is_peak_hour
from {{ ref('stg_yellow_tripdata') }}
