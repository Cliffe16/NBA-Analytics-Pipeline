USE DATABASE NBA_ANALYTICS;
USE SCHEMA STAGING;
USE WAREHOUSE NBA_WH;

SELECT
	game_id,
	game_date,
	team_name,
	home_away,
	is_win,
	points,
	field_goals_made,
	field_goals_attempted
FROM stg_team_game_results
ORDER BY team_name;
