
{{ config(materialized='table') }}

SELECT 
item_number,
lower(replace(department,'-','')) as department
from {{ ref('stg_items')  }}


