{{ config(materialized='table') }}


with time_standards as (
    select * from {{ ref('int_time_standards')  }}
)


select
    item_number,
    department,
    time_standard,
    seconds
from {{ ref('stg_items')  }} as item
left join time_standards on time_standards.time_standard = item.department
