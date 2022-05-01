
{{ config(materialized='table') }}

{{ dbt_utils.unpivot(ref('stg_time_standards'), cast_to='decimal', field_name='time_standard', value_name='seconds') }}
