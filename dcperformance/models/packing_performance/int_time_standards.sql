
{{ config(materialized='table') }}


with base_query
as (
SELECT 
(replace(time_standard,'standard','')) as time_standard,
seconds
from {{ ref('stg_time_standards_unpivot')  }})

SELECT 
lower(replace(time_standard,'_','')) as time_standard,
seconds
from base_query


