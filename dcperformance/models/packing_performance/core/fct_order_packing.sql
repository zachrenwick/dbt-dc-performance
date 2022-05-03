-- we have actual time, now calculate expected
-- going to need the departments query and the baseline time standa

-- need to add gift timing and basic packing times to this query

{{ config(materialized='table') }}

with expected_pack_times as (select * from {{ ref('int_expected_times')  }}
)



select
    dcp.order_number,
    dcp.user_id,
    dcp.order_pack_ts,
    dcp.last_order_ts,
    --dcp.order_elapsed_time,
    sum(dcp.seconds_elapsed) seconds_elapsed,
    sum(ept.expected_packing_seconds) as expected_packing_seconds,
    (sum(dcp.seconds_elapsed) - sum(ept.expected_packing_seconds)) as seconds_variance

from {{ ref('int_order_pack_times')  }} as dcp
left join
    expected_pack_times ept on ept.order_number = dcp.order_number
      and ept.user_id = dcp.user_id

group by dcp.order_number,
    dcp.user_id,
    dcp.order_pack_ts,
    dcp.last_order_ts
  --  dcp.order_elapsed_time
