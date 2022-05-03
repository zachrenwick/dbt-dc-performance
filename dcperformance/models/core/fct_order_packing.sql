{{ config(materialized='table') }}

with expected_pack_times as (

    select
        order_number,
        user_id,
        sum(expected_packing_seconds) as expected_packing_seconds
    from {{ ref('int_expected_pack_times')  }}
    group by order_number, user_id
),

actual_pack_times as (select * from {{ ref('int_actual_pack_times')  }}
),

final as (
    select
        actual_pack_times.order_number,
        actual_pack_times.user_id,
        actual_pack_times.order_pack_ts,
        actual_pack_times.last_order_ts,
        actual_pack_times.seconds_elapsed,
        expected_pack_times.expected_packing_seconds,
        (
            actual_pack_times.seconds_elapsed - expected_pack_times.expected_packing_seconds
        )::real as packing_seconds_variance

    from actual_pack_times
    left join
        expected_pack_times on
            expected_pack_times.order_number = actual_pack_times.order_number
            and expected_pack_times.user_id = actual_pack_times.user_id

)

select * from final
