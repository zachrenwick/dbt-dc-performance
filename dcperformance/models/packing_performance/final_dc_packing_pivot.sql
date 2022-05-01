
{{ config(materialized='table') }}

{%- set my_list = adapter.get_columns_in_relation(ref('int_transactions_pivot')) -%}

select
order_number,
order_line,
item_number
    {% for item in my_list %}
    , max( {{ item.name }} ) as {{ item.name }}_max_ts

    {% if not loop.last %}
        
    {% endif %}
    {% endfor %}


from {{ ref('int_transactions_pivot') }}
group by order_number, order_line, item_number