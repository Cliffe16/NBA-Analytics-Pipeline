-- Select All
WITH source AS(
	SELECT * FROM {{ source('raw', 'raw_player_game_logs') }}
),
-- Convert Uppercase column names to Snake case
cleaned AS(
	SELECT
		-- IDs
		GAME_ID::VARCHAR AS game_id,
		TRY_TO_DATE(GAME_DATE, 'MON DD, YYYY') AS game_date,
		SEASON_YEAR::VARCHAR AS season,
		PLAYER_ID::INTEGER AS player_id,
		PLAYER_NAME::VARCHAR AS player_name,
		TEAM_ID::INTEGER AS team_id,
		TEAM_ABBREVIATION::VARCHAR AS team_abbreviation,
		TEAM_NAME::VARCHAR AS team_name,
		MATCHUP::VARCHAR AS matchup,
		-- Result Flag
		CASE
			WHEN WL = 'W' THEN TRUE
			WHEN WL = 'L' THEN FALSE
			ELSE NULL
		END AS is_win,
		-- Minutes played
		COALESCE(MIN, 0)::INTEGER AS minutes_played,
		-- Shooting stats
		COALESCE(FGM, 0)::INTEGER AS field_goals_made,
		COALESCE(FGA, 0)::INTEGER AS field_goals_attempted,
		FG_PCT::FLOAT AS field_goals_pct,  -- no coalesce to preserve 'undefined' or NULL, else it means, all shots attempted were missed
		COALESCE(FG3M, 0)::INTEGER AS three_pointers_made,
		COALESCE(FG3A, 0)::INTEGER AS three_pointers_attempted,
		FG3_PCT::FLOAT AS three_point_pct,
		COALESCE(FTM, 0)::INTEGER AS free_throws_made,
		COALESCE(FTA, 0)::INTEGER AS free_throws_attempted,
		FT_PCT::FLOAT AS free_throw_pct,
		-- Counting stats
		COALESCE(OREB, 0)::INTEGER AS offensive_rebounds,
		COALESCE(DREB, 0)::INTEGER AS defensive_rebounds,
		COALESCE(REB, 0)::INTEGER AS total_rebounds,
		COALESCE(AST, 0)::INTEGER AS assists,
		COALESCE(STL, 0)::INTEGER AS steals,
		COALESCE(BLK, 0)::INTEGER AS blocks,
		COALESCE(TOV, 0)::INTEGER AS turnovers,
		COALESCE(PF, 0)::INTEGER AS personal_fouls,
		COALESCE(PTS, 0)::INTEGER AS points,
		PLUS_MINUS::INTEGER AS plus_minus,
	FROM source
	WHERE GAME_ID IS NOT NULL
		AND PLAYER_ID IS NOT NULL
),
-- Add row_num to identify duplicates
deduped AS(
	SELECT *,
		ROW_NUMBER() OVER(
			PARTITION BY game_id, player_id
			ORDER BY game_date DESC
			) AS row_num
	FROM cleaned
)
-- Select most recent records
SELECT * EXCLUDE row_num
FROM deduped
WHERE row_num = 1

