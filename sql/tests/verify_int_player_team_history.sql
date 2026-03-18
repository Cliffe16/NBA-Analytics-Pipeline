USE DATABASE NBA_ANALYTICS;
USE SCHEMA INTERMEDIATE;
USE WAREHOUSE NBA_WH;

SELECT
	player_id,
	player_name,
	team_id,
	effective_start_date,
	effective_end_date,
	is_current
FROM int_player_team_history
ORDER BY player_name, effective_start_date;
