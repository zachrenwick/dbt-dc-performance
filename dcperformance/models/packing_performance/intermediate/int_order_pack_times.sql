
{{ config(materialized='table') }}

-- need to double check there are no duplicates here!
with prior_order_times as (
SELECT order_number, user_id, min(coalesce(prior_order_ts, '1900-01-01')) as prior_order_ts
  FROM (
    SELECT order_number,
    user_id,
           LAG(timestamp_time, 1) OVER
             (PARTITION BY user_id ORDER BY timestamp_time)
             AS prior_order_ts
      FROM {{ ref('stg_transactions')  }}
      where action_code = 'PKOCLOSE'
     ORDER BY timestamp_time
       ) sub 
       
       group by order_number, user_id
       order by prior_order_ts)

, order_packed_ts as (
select order_number, user_id, max(timestamp_time) as order_pack_ts
from {{ ref('stg_transactions')  }}
where action_code = 'PKOCLOSE'
group by order_number, user_id)


,date_calcs as (
select current.order_number, 
current.user_id, 
current.order_pack_ts, 
NULLIF(prior.prior_order_ts , '1900-01-01T00:00:00') as last_order_ts
from order_packed_ts current
left join prior_order_times as prior on prior.order_number = current.order_number and prior.user_id = current.user_id
)


select 
order_number,
user_id,
order_pack_ts,
last_order_ts,
(order_pack_ts - last_order_ts)  as order_elapsed_time,
{{ dbt_utils.datediff("last_order_ts","order_pack_ts",'second') }} as seconds_elapsed
from date_calcs