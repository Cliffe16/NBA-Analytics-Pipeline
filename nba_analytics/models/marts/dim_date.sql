{{ config(
    materialized='table'
) }}

WITH date_spine AS(
	{{ dbt_utils.date_spine(
		datepart='day',
		start_date="cast('2020-01-01' as date)",
		end_date="cast('2030-12-31' as date)",
		) }}
),
-- Create date dimension
date_dimension AS(
	SELECT
		-- Surrogate Key
        CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT64) AS date_key,
		-- Natural Key
		date_day AS date,
		-- Day Attributes
		EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week_num,
		FORMAT_DATE('%A', date_day) AS day_name,
		EXTRACT(DAY FROM date_day) AS day_of_month,
		EXTRACT(DAYOFYEAR FROM date_day) AS day_of_year,
		-- Week Attributes
		EXTRACT(WEEK FROM date_day) AS week_of_year,
		-- Month Attributes
		EXTRACT(MONTH FROM date_day) AS month_num,
        FORMAT_DATE('%B', date_day) AS month_name,
        FORMAT_DATE('%Y-%m', date_day) AS year_month,
		-- Quarter Attributes
		EXTRACT(QUARTER FROM date_day) AS quarter_num,
		'Q' || EXTRACT(QUARTER FROM date_day) || ' ' || EXTRACT(YEAR FROM date_day) AS quarter_name,
		-- Year Attributes
		EXTRACT(YEAR FROM date_day) AS year,
		-- Day Flags
		CASE
			WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (0, 6) THEN TRUE
			ELSE FALSE
		END AS is_weekend,
		CASE
			WHEN EXTRACT(DAYOFWEEK FROM date_day) BETWEEN 1 AND 5 THEN TRUE
			ELSE FALSE
		END AS is_weekday,
		-- Define NBA Season(Oct-June; Oct-Dec are part of the next season)
		CASE 
			WHEN MONTH(date_day) >= 10 THEN YEAR(date_day) + 1
			ELSE YEAR(date_day)
		END AS nba_season_end_year
	FROM date_spine
)
SELECT * FROM date_dimension
