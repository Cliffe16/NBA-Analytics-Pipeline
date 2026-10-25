WITH source AS(
	SELECT * FROM {{ source('raw', 'raw_player_game_logs') }}
    --Check if running locally to restrict the scan to 7 days
    {% if target.name == 'dev' %}

    WHERE SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(GAME_DATE, 1, 10)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)

    {% endif %}
),
-- Convert Uppercase column names to Snake case
cleaned AS(
	SELECT
		-- IDs
		CAST(GAME_ID AS STRING) AS game_id,
        COALESCE(
            SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(GAME_DATE, 1, 10)),
            SAFE_CAST(GAME_DATE AS DATE)
            ) AS game_date,
		CAST(SEASON_YEAR AS STRING) AS season,
		CAST(PLAYER_ID AS INT64) AS player_id,
		CAST(PLAYER_NAME AS STRING) AS player_name,
		CAST(TEAM_ID AS INT64) AS team_id,
		CAST(TEAM_ABBREVIATION AS STRING) AS team_abbreviation,
		CAST(TEAM_NAME AS STRING) AS team_name,
		CAST(MATCHUP AS STRING) AS matchup,
		-- Result Flag
		CASE
			WHEN WL = 'W' THEN TRUE
			WHEN WL = 'L' THEN FALSE
			ELSE NULL
		END AS is_win,
		-- Minutes played
		CAST(COALESCE(MIN, 0) AS INT64) AS minutes_played,
		-- Shooting stats
		CAST(COALESCE(FGM, 0) AS INT64) AS field_goals_made,
		CAST(COALESCE(FGA, 0) AS INT64) AS field_goals_attempted,
		CAST(FG_PCT AS FLOAT64) AS field_goals_pct,  -- no coalesce to preserve 'undefined' or NULL, else it means, all shots attempted were missed
		CAST(COALESCE(FG3M, 0) AS INT64) AS three_pointers_made,
		CAST(COALESCE(FG3A, 0) AS INT64) AS three_pointers_attempted,
		CAST(FG3_PCT AS FLOAT64) AS three_point_pct,
		CAST(COALESCE(FTM, 0) AS INT64) AS free_throws_made,
		CAST(COALESCE(FTA, 0) AS INT64) AS free_throws_attempted,
		CAST(FT_PCT AS FLOAT64) AS free_throw_pct,
		-- Counting stats
		CAST(COALESCE(OREB, 0) AS INT64) AS offensive_rebounds,
		CAST(COALESCE(DREB, 0) AS INT64) AS defensive_rebounds,
		CAST(COALESCE(REB, 0) AS INT64) AS total_rebounds,
		CAST(COALESCE(AST, 0) AS INT64) AS assists,
		CAST(COALESCE(STL, 0) AS INT64) AS steals,
		CAST(COALESCE(BLK, 0) AS INT64) AS blocks,
		CAST(COALESCE(TOV, 0) AS INT64) AS turnovers,
		CAST(COALESCE(PF, 0) AS INT64) AS personal_fouls,
		CAST(COALESCE(PTS, 0) AS INT64) AS points,
		CAST(PLUS_MINUS AS INT64) AS plus_minus
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
SELECT * EXCEPT (row_num)
FROM deduped
WHERE row_num = 1
