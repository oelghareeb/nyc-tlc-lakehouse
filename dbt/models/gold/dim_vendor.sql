-- models/gold/dim_vendor.sql
select distinct
    vendorid as vendor_id,
    case
        when vendorid = 1 then 'Creative Mobile Technologies, LLC'
        when vendorid = 2 then 'Curb Mobility, LLC'
        when vendorid = 6 then 'Myle Technologies Inc'
        when vendorid = 7 then 'Helix'
        else 'Unknown'
    end as vendor_name
from {{ ref('stg_yellow_tripdata') }}
