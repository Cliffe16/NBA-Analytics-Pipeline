{{ config(
    materialized='table'
) }}

WITH games AS(
	SELECT DISTINCT
		game_id,
		game_date,
		season,
		team_id,
		CASE 
			WHEN matchup LIKE '%vs.%' THEN 'HOME'
			WHEN matchup LIKE '%@%' THEN 'AWAY'
			ELSE NULL
		END AS home_away_flag
	FROM {{ ref('stg_player_game_logs') }}
),
teams AS(
	SELECT
		g.game_id,
		g.game_date,
		g.season,
		MAX(CASE WHEN g.home_away_flag = 'HOME' THEN t.team_key END) AS home_key,
		MAX(CASE WHEN g.home_away_flag = 'AWAY' THEN t.team_key END) AS away_key,
	FROM games g
	LEFT JOIN {{ ref('dim_team') }} t
		ON g.team_id = t.team_id
	GROUP BY g.game_id, g.game_date, g.season
),
surrogate_key AS(
	SELECT 
		{{ dbt_utils.generate_surrogate_key(['game_id']) }} AS game_key,
		game_id,
		game_date,
		season,
		CASE
			WHEN MONTH(game_date) >= 10 OR MONTH(game_date) <= 4 THEN 'Regular Season'
			WHEN MONTH(game_date) IN (5, 6) THEN 'Playoffs'
			ELSE 'Preseason'
		END AS season_stage,
		home_key,
		away_key
	FROM teams
)
SELECT * FROM surrogate_key
