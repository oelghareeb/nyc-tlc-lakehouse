
  
    

    create table "iceberg"."nyc_gold"."dim_date"
      
      
    as (
      -- models/gold/dim_date.sql
select distinct
    date(tpep_pickup_datetime) as date_id,
    year(tpep_pickup_datetime) as year,
    quarter(tpep_pickup_datetime) as quarter,
    month(tpep_pickup_datetime) as month,
    day(tpep_pickup_datetime) as day,
    day_of_week(tpep_pickup_datetime) as day_of_week,  -- fixed
    week(tpep_pickup_datetime) as week,
    case when day_of_week(tpep_pickup_datetime) in (6,7) then true else false end as is_weekend
from "iceberg"."nyc_silver"."stg_yellow_tripdata"
    );

  