
{{ config(materialized='table') }}


with prior_order_times as (
SELECT order_number, user_id, min(coalesce(prior_order_ts, '1900-01-01')) as prior_order_ts
  FROM (
    SELECT order_number,
    user_id,
           LAG(timestamp_time, 1) OVER
             (PARTITION BY user_id ORDER BY timestamp_time)
             AS prior_order_ts
      FROM dcperformance.stg_transactions
     --WHERE user_id = 'USER13'
     ORDER BY timestamp_time
       ) sub 
       
       group by order_number, user_id
       order by prior_order_ts)

, order_packed_ts as (
select order_number, user_id, max(timestamp_time) as max_ts
from dcperformance.stg_transactions
--where user_id = 'USER13'
group by order_number, user_id)

select current.order_number, current.user_id, current.max_ts, prior.prior_order_ts, (max_ts - prior_order_ts) as order_elapsed_time
from order_packed_ts current
left join prior_order_times  prior on prior.order_number = current.order_number
order by max_ts