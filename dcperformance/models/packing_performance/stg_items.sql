/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

SELECT
    "MATERIAL" AS ITEM_NUMBER,
    "DEPARTMENT" AS DEPARTMENT

FROM {{ ref('article_master')  }}
