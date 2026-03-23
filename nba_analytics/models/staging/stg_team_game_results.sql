WITH source AS(
	SELECT * FROM {{ source('raw', 'raw_team_game_results') }}
),
cleaned AS(
	SELECT
        	-- IDs
		GAME_ID::VARCHAR AS game_id,
        	COALESCE(
                TRY_TO_DATE(SUBSTR(GAME_DATE, 1, 10), 'YYYY-MM-DD'),
                TRY_CAST(GAME_DATE AS DATE)) AS game_date,
        	SEASON_ID::VARCHAR AS season,
        	TEAM_ID::INTEGER AS team_id,
        	TEAM_ABBREVIATION::VARCHAR AS team_abbreviation,
        	TEAM_NAME::VARCHAR AS team_name,
        	MATCHUP::VARCHAR AS matchup,
        	-- Win/loss flag
        	CASE
            		WHEN WL = 'W' THEN TRUE
            		WHEN WL = 'L' THEN FALSE
            		ELSE NULL
        	END AS is_win,
        	-- Home/Away flag
        	CASE
            		WHEN MATCHUP LIKE '%vs.%' THEN 'HOME'
            		WHEN MATCHUP LIKE '%@%' THEN 'AWAY'
            		ELSE NULL
        		END AS home_away,
        	-- Minutes(240 for regulation, more for OT)
        	COALESCE(MIN, 0)::INTEGER AS minutes_played,
        	-- Team stats
        	COALESCE(PTS, 0)::INTEGER AS points,
        	COALESCE(FGM, 0)::INTEGER AS field_goals_made,
        	COALESCE(FGA, 0)::INTEGER AS field_goals_attempted,
        	FG_PCT::FLOAT AS field_goals_pct,
        	COALESCE(FG3M, 0)::INTEGER AS three_pointers_made,
        	COALESCE(FG3A, 0)::INTEGER AS three_pointers_attempted,
        	FG3_PCT::FLOAT AS three_point_pct,
        	COALESCE(FTM, 0)::INTEGER AS free_throws_made,
        	COALESCE(FTA, 0)::INTEGER AS free_throws_attempted,
        	FT_PCT::FLOAT AS free_throw_pct,
        	COALESCE(OREB, 0)::INTEGER AS offensive_rebounds,
        	COALESCE(DREB, 0)::INTEGER AS defensive_rebounds,
        	COALESCE(REB, 0)::INTEGER AS total_rebounds,
        	COALESCE(AST, 0)::INTEGER AS assists,
        	COALESCE(STL, 0)::INTEGER AS steals,
        	COALESCE(BLK, 0)::INTEGER AS blocks,
        	COALESCE(TOV, 0)::INTEGER AS turnovers,
        	COALESCE(PF, 0)::INTEGER AS personal_fouls,
        	PLUS_MINUS::INTEGER AS plus_minus,

        	-- Metadata
        	loaded_at
    FROM source
    WHERE GAME_ID IS NOT NULL
        AND TEAM_ID IS NOT NULL
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY game_id, team_id
            ORDER BY loaded_at DESC
        ) AS row_num
    FROM cleaned
)

SELECT * EXCLUDE row_num
FROM deduped
WHERE row_num = 1
