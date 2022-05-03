--union basicpack, gift records and order qtys into one table, 
--to be joined against the time standards unpivot

{{ config(materialized='table') }}


-- the first union can be modified to change assumptions around how much setup time is needed per order
with other_items_and_order_qtys
as (
    select distinct
        order_number,
        user_id,
        'basicpack' as item,
        1 as quantity -- 1 'basicpack' per order
    from {{ ref('stg_transactions')  }}
    where action_code = 'PKOCLOSE'

    union

    select
        order_number, -- not distinct as an order can have multiple gift items 
        user_id,
        'giftitem' as item,
        quantity
    from {{ ref('stg_transactions')  }}
    where action_code = 'PKOCLOSE'
        and gift_flag = '1'

    union all

    select
        order_number,
        user_id,
        item_number::text as item,
        sum(quantity) as quantity
    from {{ ref('stg_transactions')  }}
    where action_code = 'PKOCLOSE'
    group by order_number, user_id, item

),

item_time_standards as (

    select
        seconds,
        coalesce(item_number::text, time_standard) as item
    from {{ ref('int_time_standards')  }} as time_standards
    left join {{ ref('stg_items')  }} as item
        on
            time_standards.time_standard = item.department
),


final as (
    select
        other_items_and_order_qtys.item,
        other_items_and_order_qtys.order_number,
        other_items_and_order_qtys.user_id,
        other_items_and_order_qtys.quantity,
        item_time_standards.seconds,
        other_items_and_order_qtys.quantity * item_time_standards.seconds as expected_packing_seconds

    from other_items_and_order_qtys
    left join
        item_time_standards on
            item_time_standards.item = other_items_and_order_qtys.item
)


select * from final
