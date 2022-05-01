
{{ config(materialized='table') }}


{% set action_codes = dbt_utils.get_column_values(table=ref('stg_transactions'), column='action_code') %}

select 
order_number,
item_number
--min or max of each column ts value!
{% for action_code in action_codes %}
    ,max(timestamp_time)  as {{action_code}}_max_ts
    {% endfor %}

-- for each column in column_list, min(column_name) // end 
from {{ ref('stg_transactions')  }}
group by order_number,
         order_line,
         item_number





