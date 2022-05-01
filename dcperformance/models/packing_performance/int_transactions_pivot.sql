
{{ config(materialized='table') }}

{% set action_codes = dbt_utils.get_column_values(table=ref('stg_transactions'), column='action_code') %}

SELECT 
order_number,
order_line,
item_number
    {% for action_code in action_codes %}
    ,case when action_code = '{{action_code}}' then timestamp_time end as {{action_code}}_ts
    {% endfor %}
    
FROM {{ ref('stg_transactions') }}
 
