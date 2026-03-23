{% test valid_season_date(model, column_name) %}

-- Fails if the date falls outside the bounds of the 2024-2025 NBA season
SELECT *
FROM {{ model }}
WHERE {{ column_name }} < '2024-10-01'::DATE 
   OR {{ column_name }} > '2025-06-30'::DATE

{% endtest %}
