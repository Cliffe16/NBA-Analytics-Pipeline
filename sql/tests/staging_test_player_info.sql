USE DATABASE NBA_ANALYTICS;
USE SCHEMA STAGING;
USE WAREHOUSE NBA_WH;

SELECT
	player_id,
	player_name,
	birth_date,
	height,
	height_inches,
	weight,
	weight_pounds,
	position,
	current_team_abbreviation,
	season_experience
FROM stg_player_info
ORDER BY player_name;
