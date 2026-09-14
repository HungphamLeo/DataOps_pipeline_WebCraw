-- dbt_project/models/silver/fact_stock_price.sql
-- Silver model: deduplicate và normalize raw bronze stock_prices
-- TODO: implement khi bronze data ổn định

{{
  config(
    materialized='table',
    schema='silver'
  )
}}

-- Placeholder — sẽ được implement sau khi bronze pipeline ổn định
select 1 as placeholder where false
