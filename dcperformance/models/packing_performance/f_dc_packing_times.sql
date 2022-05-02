-- we have actual time, now calculate expected
-- going to need the departments query and the baseline time standa
{{ config(materialized='table') }}

with expected_pack_times as (select * from {{ ref('int_expected_times')  }}
)

select
    dcp.order_number,
    dcp.user_id,
    dcp.order_pack_ts,
    dcp.prior_order_ts,
    dcp.order_elapsed_time,
    dcp.seconds_elapsed,
    expected_pack_times.total_expected_order_packing_seconds,
    (
        dcp.seconds_elapsed - expected_pack_times.total_expected_order_packing_seconds
    )::decimal as seconds_variance

from {{ ref('fact_dc_packing')  }} as dcp
left join
    expected_pack_times on expected_pack_times.order_number = dcp.order_number
