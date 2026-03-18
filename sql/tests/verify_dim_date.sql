USE DATABASE NBA_ANALYTICS;
USE SCHEMA MARTS;
USE WAREHOUSE NBA_WH;

-- Check row count
SELECT COUNT(*) FROM dim_date;

-- Show sample 
SELECT
	date_key,
	date,
	day_name,
 	month_name,
	quarter_name,
	is_weekend,
	nba_season_end_year
FROM dim_date
WHERE date BETWEEN '2024-10-01' AND '2024-10-31'
ORDER BY date;
