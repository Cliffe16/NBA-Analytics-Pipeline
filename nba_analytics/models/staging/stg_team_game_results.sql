WITH source AS(
	SELECT * FROM {{ source('raw', 'raw_team_game_results') }}
    {% if target.name == 'dev' %}
    --Check if running locally to restrict the scan to 7 days
    WHERE SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(GAME_DATE, 1, 10)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)

    {% endif %}
),
cleaned AS(
	SELECT
        	-- IDs
		CAST(GAME_ID AS STRING) AS game_id,
        	COALESCE(
                SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(GAME_DATE, 1, 10)),
                SAFE_CAST(GAME_DATE AS DATE)
            ) AS game_date,
        	CAST(SEASON_ID AS STRING) AS season,
        	CAST(TEAM_ID AS INT64) AS team_id,
        	CAST(TEAM_ABBREVIATION AS STRING) AS team_abbreviation,
        	CAST(TEAM_NAME AS STRING) AS team_name,
        	CAST(MATCHUP AS STRING) AS matchup,
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
        	CAST(COALESCE(MIN, 0) AS INT64) AS minutes_played,
        	-- Team stats
        	CAST(COALESCE(PTS, 0) AS INT64) AS points,
        	CAST(COALESCE(FGM, 0) AS INT64) AS field_goals_made,
        	CAST(COALESCE(FGA, 0) AS INT64) AS field_goals_attempted,
        	CAST(FG_PCT AS FLOAT64) AS field_goals_pct,
        	CAST(COALESCE(FG3M, 0) AS INT64) AS three_pointers_made,
        	CAST(COALESCE(FG3A, 0) AS INT64) AS three_pointers_attempted,
        	CAST(FG3_PCT AS FLOAT64) AS three_point_pct,
        	CAST(COALESCE(FTM, 0) AS INT64) AS free_throws_made,
        	CAST(COALESCE(FTA, 0) AS INT64) AS free_throws_attempted,
        	CAST(FT_PCT AS FLOAT64) AS free_throw_pct,
        	CAST(COALESCE(OREB, 0) AS INT64) AS offensive_rebounds,
        	CAST(COALESCE(DREB, 0) AS INT64) AS defensive_rebounds,
        	CAST(COALESCE(REB, 0) AS INT64) AS total_rebounds,
        	CAST(COALESCE(AST, 0) AS INT64) AS assists,
        	CAST(COALESCE(STL, 0) AS INT64) AS steals,
        	CAST(COALESCE(BLK, 0) AS INT64) AS blocks,
        	CAST(COALESCE(TOV, 0) AS INT64) AS turnovers,
        	CAST(COALESCE(PF, 0) AS INT64) AS personal_fouls,
        	CAST(PLUS_MINUS AS INT64) AS plus_minus,

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
        ) AS rn
    FROM cleaned
)

SELECT * EXCEPT (rn)
FROM deduped
WHERE rn = 1
