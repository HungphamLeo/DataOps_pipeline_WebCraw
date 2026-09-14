-- dbt_project/models/gold/mart_stock_summary.sql
-- Gold model: aggregate silver data thành mart layer
-- TODO: implement khi silver models ổn định

{{
  config(
    materialized='table',
    schema='gold'
  )
}}

-- Placeholder — sẽ được implement sau khi silver pipeline ổn định
select 1 as placeholder where false
