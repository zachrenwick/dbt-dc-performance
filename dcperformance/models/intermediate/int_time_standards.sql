
{{ config(materialized='table') }}


with unpivot as 
({{ dbt_utils.unpivot(ref('stg_time_standards'), cast_to='decimal', field_name='time_standard', value_name='seconds') }})

,final
as (
    select
        seconds,
        lower(replace(replace(time_standard, 'standard', ''),'_', '')) as time_standard
    from unpivot
)


select * from final