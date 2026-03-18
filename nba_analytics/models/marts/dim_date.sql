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
		TO_CHAR(date_day, 'YYYYMMDD')::INTEGER AS date_key,
		-- Natural Key
		date_day AS date,
		-- Day Attributes
		DAYOFWEEK(date_day) AS day_of_week_num,
		DAYNAME(date_day) AS day_name,
		DAYOFMONTH(date_day) AS day_of_month,
		DAYOFYEAR(date_day) AS day_of_year,
		-- Week Attributes
		WEEKOFYEAR(date_day) AS week_of_year,
		-- Month Attributes
		MONTH(date_day) AS month_num,
		MONTHNAME(date_day) AS month_name,
		TO_CHAR(date_day, 'YYYY-MM') AS year_month,
		-- Quarter Attributes
		QUARTER(date_day) AS quarter_num,
		'Q' || QUARTER(date_day) || ' ' || YEAR(date_day) AS quarter_name,
		-- Year Attributes
		YEAR(date_day) AS year,
		-- Day Flags
		CASE
			WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE
			ELSE FALSE
		END AS is_weekend,
		CASE
			WHEN DAYOFWEEK(date_day) BETWEEN 1 AND 5 THEN TRUE
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
