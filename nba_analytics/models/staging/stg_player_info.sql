WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_player_info') }}
),

cleaned AS(
        SELECT
                -- IDs
                CAST(PERSON_ID AS INT64) AS player_id,
                CAST(FIRST_NAME AS STRING) AS first_name,
                CAST(LAST_NAME AS STRING) AS last_name,
                CAST(DISPLAY_FIRST_LAST AS STRING) AS player_name,
                
                -- Biographical Info
                SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(CAST(BIRTHDATE AS STRING), 1, 10)) AS birth_date,
                CAST(SCHOOL AS STRING) AS school,
                CAST(COUNTRY AS STRING) AS country,
                
                -- Physical Attributes
                CAST(HEIGHT AS STRING) AS height,
                CAST(WEIGHT AS STRING) AS weight,
                
                -- convert height to inches
                CASE
                        WHEN HEIGHT LIKE '%-%' THEN
                                (CAST(SPLIT(HEIGHT, '-')[SAFE_OFFSET(0)] AS INT64) * 12) +
                                CAST(SPLIT(HEIGHT, '-')[SAFE_OFFSET(1)] AS INT64)
                        ELSE NULL
                END AS height_inches,
                
                -- convert weight to integer 
                SAFE_CAST(WEIGHT AS INT64) AS weight_pounds,
                CAST(POSITION AS STRING) AS position,
                CAST(JERSEY AS STRING) AS jersey_number,
                CAST(SEASON_EXP AS STRING) AS season_experience,
                
                -- Current team
                CAST(TEAM_ID AS INT64) AS current_team_id,
                CAST(TEAM_NAME AS STRING) AS current_team_name,
                CAST(TEAM_ABBREVIATION AS STRING) AS current_team_abbreviation,
                
                -- Status
                CAST(ROSTERSTATUS AS STRING) AS roster_status,
                
                -- Career span
                CAST(FROM_YEAR AS INT64) AS career_start_year,
                CAST(TO_YEAR AS INT64) AS career_end_year,
                
                -- Draft info
                CAST(DRAFT_YEAR AS STRING) AS draft_year,
                CAST(DRAFT_ROUND AS STRING) AS draft_round,
                CAST(DRAFT_NUMBER AS STRING) AS draft_number,
                
                -- Metadata
                CURRENT_TIMESTAMP() AS loaded_at
        FROM source
        WHERE PERSON_ID IS NOT NULL
),

deduped AS(
        SELECT *,
                ROW_NUMBER() OVER(
                        PARTITION BY player_id
                        ORDER BY loaded_at DESC
                        ) AS rn
        FROM cleaned
)
SELECT * EXCEPT (rn)
FROM deduped
WHERE rn = 1
