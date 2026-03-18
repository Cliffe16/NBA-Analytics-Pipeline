USE DATABASE NBA_ANALYTICS;
USE SCHEMA INTERMEDIATE;
USE WAREHOUSE NBA_WH;

SELECT
	game_id,
	game_date,
	home_team_abbr,
	home_points,
	away_team_abbr,
	away_points,
	winning_team_id,
	point_differential
FROM int_game_results;
