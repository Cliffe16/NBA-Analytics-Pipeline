{% test assert_data_freshness(model, column_name, days_back=3) %}

-- This test fails if the most recent date in the table is older than 'days_back'
WITH max_date_cte AS (
    SELECT MAX(CAST({{ column_name }} AS DATE)) AS max_date_val
    FROM {{ model }}
)

SELECT *
FROM max_date_cte
WHERE max_date_val < DATE_SUB(CURRENT_DATE(), INTERVAL {{ days_back }} DAY)
   OR max_date_val IS NULL

{% endtest %}
