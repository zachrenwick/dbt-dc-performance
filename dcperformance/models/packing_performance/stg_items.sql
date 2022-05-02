{{ config(materialized='table') }}

SELECT
    "MATERIAL" AS item_number,
    lower(replace("DEPARTMENT", '-', '')) AS department

FROM {{ ref('article_master')  }}