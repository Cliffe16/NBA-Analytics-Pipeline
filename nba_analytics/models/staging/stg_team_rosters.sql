WITH source AS(
	SELECT * FROM {{ source('raw', 'raw_team_rosters') }}
),
cleaned AS(
	SELECT
        	-- Snapshot metadata
        	snapshot_date::DATE AS snapshot_date,
        	-- IDs
        	TeamID::INTEGER AS team_id,
        	PLAYER_ID::INTEGER AS player_id,
        	PLAYER::VARCHAR AS player_name,
        	SEASON::VARCHAR AS season,
        	-- Player details
        	NUM::VARCHAR AS jersey_number,
        	POSITION::VARCHAR AS position,
        	-- Physical attributes
        	HEIGHT::VARCHAR AS height,
        	WEIGHT::VARCHAR AS weight,
        	-- Convert height to inches
        	CASE
            		WHEN HEIGHT LIKE '%-%' THEN
                		(SPLIT_PART(HEIGHT, '-', 1)::INTEGER * 12) +
                		SPLIT_PART(HEIGHT, '-', 2)::INTEGER
            		ELSE NULL
        	END AS height_inches,
        	-- Convert weight to integer
        	TRY_CAST(WEIGHT AS INTEGER) AS weight_pounds,
        	-- Biographical
        	TRY_TO_DATE(BIRTH_DATE) AS birth_date,
        	AGE::VARCHAR AS age,
        	EXP::VARCHAR AS years_experience,
        	SCHOOL::VARCHAR AS school,
        	HOW_ACQUIRED::VARCHAR AS how_acquired,
        	-- Metadata
        	loaded_at
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
        		) AS row_num
	FROM cleaned
)

SELECT * EXCLUDE row_num
FROM deduped
WHERE row_num = 1
