-- need to add the standard times for each (set up standard pack time)  ++ gift qty time
-- also add in gift tag


{{ config(materialized='table') }}

with item_time_standards as (

    select
        item_number,
        seconds
    from {{ ref('stg_items')  }} as item
    left join
        {{ ref('int_time_standards')  }} as time_standards on
            time_standards.time_standard = item.department 
),


order_qtys as (select
    order_number,
    user_id,
    item_number,
    sum(quantity) as item_qty
    from {{ ref('stg_transactions')  }}
    where action_code = 'PKOCLOSE'
    group by order_number, user_id, item_number
),

expected_by_order_item as (
    select
        order_qtys.item_number,
        order_qtys.order_number,
        order_qtys.user_id,
        item_qty,
        seconds,
        item_qty * seconds as expected_packing_seconds

    from order_qtys
    left join
        item_time_standards on
            item_time_standards.item_number = order_qtys.item_number
)


select * from expected_by_order_item
