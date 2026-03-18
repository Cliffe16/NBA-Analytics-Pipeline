USE DATABASE NBA_ANALYTICS;
USE SCHEMA STAGING;
USE WAREHOUSE NBA_WH;

SELECT
	snapshot_date,
	team_id,
	player_id,
	player_name,
	position,
	height,
	height_inches,
	weight,
	weight_pounds,
	how_acquired
FROM stg_team_rosters
ORDER BY player_name;
