{{ config(
    materialized='table'
) }}


WITH player_game_logs AS(
	SELECT * FROM {{ ref('stg_player_game_logs') }}
),
-- Calculate player metrics
metrics AS(
	SELECT *,
		-- True Shooting Percentage: TOTAL PTS / (2 * (FGA + 0.44*FTA))
		CASE
			WHEN (field_goals_attempted + (0.44 * free_throws_attempted)) > 0
				THEN points / (2.0 * field_goals_attempted + (0.44 * free_throws_attempted))
			ELSE NULL
		END AS true_shooting_pct,
		-- Game Score (metric measuring a player's single-game productivity based on box score stats)
		(points + (0.4 * field_goals_made) - (0.7 * field_goals_attempted) - (0.4 * (free_throws_attempted - free_throws_made)) 
			+ (0.7 * offensive_rebounds) + (0.3 * defensive_rebounds) + steals + (0.7 * assists) + (0.7 * blocks) 
            		- (0.4 * personal_fouls) - turnovers
		) AS game_score,
		-- Game number for player this season
		ROW_NUMBER() OVER( PARTITION BY player_id, season ORDER BY game_date) AS season_game_number
	FROM player_game_logs
),
season_highs AS(
	SELECT *,
		-- Determine season highs with boolean flags
		-- points
		CASE 
			WHEN points = MAX(points) OVER (PARTITION BY player_id, season) THEN TRUE
			ELSE FALSE
		END AS is_season_high_points,
		-- rebounds
		CASE 
			WHEN total_rebounds = MAX(total_rebounds) OVER (PARTITION BY player_id, season) THEN TRUE 
			ELSE FALSE 
		END AS is_season_high_rebounds,
		-- assists
		CASE 
			WHEN assists = MAX(assists) OVER (PARTITION BY player_id, season) THEN TRUE 
			ELSE FALSE 
		END AS is_season_high_assists,
		--blocks
		CASE 
                        WHEN blocks = MAX(blocks) OVER (PARTITION BY player_id, season) THEN TRUE 
                        ELSE FALSE 
                END AS is_season_high_blocks,
		-- fouls
		CASE 
                        WHEN personal_fouls = MAX(personal_fouls) OVER (PARTITION BY player_id, season) THEN TRUE 
                        ELSE FALSE 
                END AS is_season_high_fouls,
		-- steals
		CASE 
                        WHEN steals = MAX(steals) OVER (PARTITION BY player_id, season) THEN TRUE 
                        ELSE FALSE 
                END AS is_season_high_steals,
		-- three pointers made
		CASE 
                        WHEN three_pointers_made = MAX(three_pointers_made) OVER (PARTITION BY player_id, season) THEN TRUE 
                        ELSE FALSE 
                END AS is_season_high_3p,
		-- turnovers
		CASE 
                        WHEN turnovers = MAX(turnovers) OVER (PARTITION BY player_id, season) THEN TRUE 
                        ELSE FALSE 
                END AS is_season_high_turnovers
	FROM metrics
),
keys AS(
	SELECT
		-- Surrogate Key 
        	{{ dbt_utils.generate_surrogate_key(['s.game_id', 's.player_id']) }} AS player_fact_key,
        	-- Dimension Foreign Keys
        	g.game_key,
        	p.player_key,
        	t.team_key,
        	d.date_key,
        	-- Game Context
        	s.season_game_number,
        	-- Base Playing Stats
        	s.minutes_played,
        	s.points,
        	s.field_goals_made,
        	s.field_goals_attempted,
        	s.field_goals_pct,  
        	s.three_pointers_made,
        	s.three_pointers_attempted,
        	s.three_point_pct,
        	s.free_throws_made,
        	s.free_throws_attempted,
        	s.free_throw_pct,
        	s.offensive_rebounds,
        	s.defensive_rebounds,
        	s.total_rebounds,
        	s.assists,
        	s.steals,
        	s.blocks,
        	s.turnovers,
        	s.personal_fouls,
        	s.plus_minus,
        	-- Calculated Advanced Metrics
        	s.true_shooting_pct,
        	s.game_score,
        	-- Season High Flags
        	s.is_season_high_points,
        	s.is_season_high_rebounds,
        	s.is_season_high_assists,
		s.is_season_high_blocks,
                s.is_season_high_fouls,
                s.is_season_high_steals,
		s.is_season_high_3p,
                s.is_season_high_turnovers
	FROM season_highs s
    		LEFT JOIN {{ ref('dim_game') }} g 
        		ON s.game_id = g.game_id
    				LEFT JOIN {{ ref('dim_team') }} t 
        				ON s.team_id = t.team_id
    						LEFT JOIN {{ ref('dim_date') }} d 
        						ON TO_CHAR(s.game_date, 'YYYYMMDD')::INTEGER = d.date_key
        							-- find player record valid on game date
    								LEFT JOIN {{ ref('dim_player') }} p 
        								ON s.player_id = p.player_id 
        									AND s.game_date >= p.effective_start_date
        									AND s.game_date < p.effective_end_date
)
SELECT * FROM keys
