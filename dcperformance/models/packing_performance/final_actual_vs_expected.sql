-- we have actual time, now calculate expected

-- going to need the departments query and the baseline time standa


with expected_pack_times as 
(select * from  dcperformance.int_expected_times
)

select 
dcp.order_number,
dcp.user_id,
dcp.max_ts,
dcp.prior_order_ts,
dcp.order_elapsed_time,
ept.total_expected_order_packing_seconds

from dcperformance.fact_dc_packing dcp
left join expected_pack_times ept on ept.order_number = dcp.order_number