{{ config(materialized='table') }}

with expected_pack_times as (
        
    select order_number, 
    user_id, 
    sum(expected_packing_seconds) as expected_packing_seconds
    from {{ ref('int_expected_pack_times')  }}
    group by order_number, user_id
)
,
actual_pack_times as (select * from  {{ ref('int_actual_pack_times')  }}),

final as (
select
    apt.order_number,
    apt.user_id,
    apt.order_pack_ts,
    apt.last_order_ts,
    apt.seconds_elapsed ,
    ept.expected_packing_seconds, 
    (apt.seconds_elapsed - ept.expected_packing_seconds)::real  as packing_seconds_variance

from actual_pack_times as apt
left join
    expected_pack_times ept on ept.order_number = apt.order_number
                           and ept.user_id = apt.user_id

)

select * from final
