
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

select 
order_number,
order_line,
item_number,
{{ dbt_utils.pivot('timestamp_time',  dbt_utils.get_column_values(table=ref('stg_transactions'), column='action_code')) }}

from {{ ref('stg_transactions')  }}
group by order_number,
         order_line,
         item_number


