
{{ config(materialized='table') }}


with base_query
as (
    select
        seconds,
        (replace(time_standard, 'standard', '')) as time_standard
    from {{ ref('stg_time_standards_unpivot')  }}
)

select
    seconds,
    lower(replace(time_standard, '_', '')) as time_standard
from base_query
