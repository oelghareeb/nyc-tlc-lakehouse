-- models/gold/dim_rate_code.sql
select distinct
    ratecodeid as rate_code_id,
    case
        when ratecodeid = 1 then 'Standard rate'
        when ratecodeid = 2 then 'JFK'
        when ratecodeid = 3 then 'Newark'
        when ratecodeid = 4 then 'Nassau or Westchester'
        when ratecodeid = 5 then 'Negotiated fare'
        when ratecodeid = 6 then 'Group ride'
        when ratecodeid = 99 then 'Unknown'
        else 'Other'
    end as rate_code_desc
from "iceberg"."nyc_silver"."stg_yellow_tripdata"