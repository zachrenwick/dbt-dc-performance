
{{ config(materialized='table') }}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('int_transactions_pivot'), except=["order_number", "order_line", "item_number", "user_id"]) %}

select
order_number,
order_line,
item_number,
user_id
    {% for column_name in column_names %}
    , max( {{ column_name }} ) as {{ column_name }}_max
    {% endfor %}
from {{ ref('int_transactions_pivot') }}
group by order_number, order_line, item_number, user_id