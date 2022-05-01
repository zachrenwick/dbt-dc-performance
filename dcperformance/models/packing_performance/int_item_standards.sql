{{ config(materialized='table') }}


with time_standards as (
select * from {{ ref('int_time_standards')  }})


select item_number, department, time_standard, seconds
from {{ ref('int_items')  }} item 
left join time_standards ts on ts.time_standard = item.department