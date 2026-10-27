{% test valid_season_date(model, column_name) %}

-- Fails if the date falls outside the bounds of the 2024-2025 NBA season
SELECT *
FROM {{ model }}
WHERE {{ column_name }} < CAST('2024-10-01' AS DATE)
   OR {{ column_name }} > CAST('2025-06-30' AS DATE)

{% endtest %}
