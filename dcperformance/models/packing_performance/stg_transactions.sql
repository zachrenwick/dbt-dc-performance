
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

WITH base_transactions as (
select *
from {{ ref('transactions')  }})

SELECT 

"TRANSACTION_ID" AS trans_id,
"TRANSACTION_TIMESTAMP" AS timestamp_time,
"DATE" AS timestamp_date,
"ORDER_TYPE_CATEGORY" AS order_category,
"ORDER_CHANNEL" AS channel,
"USER_ID" AS user_id,
"ACTION_CODE" AS action_code,
"ORDER_NUMBER" AS order_number,
"ORDER_LINE_ITEM" AS order_line,
"ARTICLE" AS item_number

from base_transactions



