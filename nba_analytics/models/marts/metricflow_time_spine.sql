{{ config(materialized='table') }}

-- MetricFlow requires a dedicated time spine model to handle time-series aggregations
SELECT
    date AS date_day
FROM {{ ref('dim_date') }}
