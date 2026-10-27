WITH source AS(
	SELECT * FROM {{ source('raw', 'raw_team_rosters') }}
),
cleaned AS(
	SELECT
        	-- Snapshot metadata
        	CAST(snapshot_date AS DATE) AS snapshot_date,
        	-- IDs
        	CAST(TeamID AS INT64) AS team_id,
        	CAST(PLAYER_ID AS INT64) AS player_id,
        	CAST(PLAYER AS STRING) AS player_name,
        	CAST(SEASON AS STRING) AS season,
        	-- Player details
        	CAST(NUM AS STRING) AS jersey_number,
        	CAST(POSITION AS STRING) AS position,
        	-- Physical attributes
        	CAST(HEIGHT AS STRING) AS height,
        	CAST(WEIGHT AS STRING) AS weight,
        	-- Convert height to inches
        	CASE
            		WHEN HEIGHT LIKE '%-%' THEN
                		(CAST(SPLIT(HEIGHT, '-')[SAFE_OFFSET(0)] AS INT64) * 12) +
                		CAST(SPLIT(HEIGHT, '-')[SAFE_OFFSET(1)] AS INT64)
            		ELSE NULL
        	END AS height_inches,
        	-- Convert weight to integer
        	SAFE_CAST(WEIGHT AS INT64) AS weight_pounds,
        	-- Biographical
        	SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(CAST(BIRTH_DATE AS STRING), 1, 10)) AS birth_date,
        	CAST(AGE AS STRING) AS age,
        	CAST(EXP AS STRING) AS years_experience,
        	CAST(SCHOOL AS STRING) AS school,
        	CAST(HOW_ACQUIRED AS STRING) AS how_acquired,
        	-- Metadata
        	CURRENT_TIMESTAMP() AS loaded_at
	FROM source
	WHERE PLAYER_ID IS NOT NULL
        	AND TeamID IS NOT NULL
        	AND snapshot_date IS NOT NULL
),

deduped AS(
	SELECT *,
        	ROW_NUMBER() OVER(
            		PARTITION BY snapshot_date, player_id, team_id
            		ORDER BY loaded_at DESC
        		) AS rn
	FROM cleaned
)

SELECT * EXCEPT (rn)
FROM deduped
WHERE rn = 1
