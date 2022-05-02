
{{ config(materialized='table') }}

SELECT
    item_number,
    lower(replace(department, '-', '')) AS department
FROM {{ ref('stg_items')  }}
