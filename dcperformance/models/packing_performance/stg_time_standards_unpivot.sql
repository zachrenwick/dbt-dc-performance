
{{ config(materialized='table') }}

{{ dbt_utils.unpivot(ref('stg_time_standards'), cast_to='decimal') }}
