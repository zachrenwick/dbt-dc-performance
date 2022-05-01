-- we have actual time, now calculate expected

-- going to need the departments query and the baseline time standards
-- intermediate query to calc expected packing time
-- time standards * item id * qty
{{ config(materialized='table') }}

with item_time_standards as
(select item_number, seconds from {{ ref('int_item_standards')  }}),

order_qtys as 
(select order_number, user_id, item_number, sum(quantity) as qty
from {{ ref('stg_transactions')  }}
where action_code = 'PKOCLOSE'
group by order_number, user_id, item_number),

expected_by_order_item as (
select 
oq.item_number,
oq.order_number,
oq.user_id,
qty,
seconds,
qty * seconds as expected_packing_seconds

from order_qtys oq 
left join item_time_standards ts on ts.item_number = oq.item_number),


final_query as

(select order_number,
sum(expected_packing_seconds) as total_expected_order_packing_seconds
from expected_by_order_item
group by order_number)


select * from final_query