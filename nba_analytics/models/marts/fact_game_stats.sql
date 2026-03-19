{{ config(
    materialized='table'
) }}

WITH game_results AS(
	SELECT * FROM {{ ref('int_game_results') }}
),
teams AS(
	-- Home Team row
	SELECT
        	game_id,
        	game_date,
        	home_team_id AS team_id,
        	away_team_id AS opponent_team_id,
        	home_points AS points_scored,
        	away_points AS points_allowed,
        	-- Percentages
        	CASE 
			WHEN home_fga > 0 THEN (home_fgm::FLOAT / home_fga) * 100.0
			ELSE 0 
		END AS field_goals_pct,
        	CASE 
			WHEN home_fg3a > 0 THEN (home_fg3m::FLOAT / home_fg3a) * 100.0
			ELSE 0 
		END AS three_point_pct,
        	CASE 
			WHEN home_fta > 0 THEN (home_ftm::FLOAT / home_fta) * 100.0
			ELSE 0 
		END AS free_throw_pct,
        	home_rebounds AS total_rebounds,
        	home_assists AS assists,
        	home_steals AS steals,
        	home_blocks AS blocks,
        	home_turnovers AS turnovers,
        	-- Determine if the home team won
        	CASE 
			WHEN winning_team_id = home_team_id THEN TRUE 
			ELSE FALSE 
		END AS is_win
	FROM game_results

		UNION ALL

    -- Away Team 
	SELECT
        	game_id,
        	game_date,
        	away_team_id AS team_id,
        	home_team_id AS opponent_team_id,
        	away_points AS points_scored,
        	home_points AS points_allowed,
        	-- Percentages
        	CASE 
			WHEN away_fga > 0 THEN (away_fgm::FLOAT / away_fga) * 100.0
			ELSE 0 
		END AS field_goals_pct,
        	CASE 
			WHEN away_fg3a > 0 THEN (away_fg3m::FLOAT / away_fg3a) * 100.0
			ELSE 0 
		END AS three_point_pct,
        	CASE 
			WHEN away_fta > 0 THEN (away_ftm::FLOAT / away_fta) * 100.0
			ELSE 0 
		END AS free_throw_pct,
        	away_rebounds AS total_rebounds,
        	away_assists AS assists,
        	away_steals AS steals,
        	away_blocks AS blocks,
        	away_turnovers AS turnovers,
        	-- Determine if the away team won
        	CASE 
			WHEN winning_team_id = away_team_id THEN TRUE 
			ELSE FALSE 
		END AS is_win
	FROM game_results
),
keys AS(
	SELECT
        	-- Surrogate Key (Uniquely identifies one team's performance in one game)
        	{{ dbt_utils.generate_surrogate_key(['t.game_id', 't.team_id']) }} AS fact_key,
        	-- Dimension Foreign Keys
        	g.game_key,
        	dt.team_key,
        	opp.team_key AS opponent_team_key,
        	d.date_key,
        	-- Game Measures
        	t.points_scored,
        	t.points_allowed,
        	t.field_goals_pct,
        	t.three_point_pct,
        	t.free_throw_pct,
        	t.total_rebounds,
        	t.assists,
        	t.steals,
        	t.blocks,
        	t.turnovers,
        	t.is_win 
    	FROM teams t
    		LEFT JOIN {{ ref('dim_game') }} g 
        		ON t.game_id = g.game_id
    				LEFT JOIN {{ ref('dim_team') }} dt 
        				ON t.team_id = dt.team_id
    						LEFT JOIN {{ ref('dim_team') }} opp 
        						ON t.opponent_team_id = opp.team_id
    								LEFT JOIN {{ ref('dim_date') }} d 
        								ON TO_CHAR(t.game_date, 'YYYYMMDD')::INTEGER = d.date_key
)

SELECT * FROM keys
