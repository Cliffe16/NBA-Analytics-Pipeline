WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_player_info') }}
),

cleaned AS(
        SELECT
                -- IDs
                PERSON_ID::INTEGER AS player_id,
                FIRST_NAME::VARCHAR AS first_name,
                LAST_NAME::VARCHAR AS last_name,
                DISPLAY_FIRST_LAST::VARCHAR AS player_name,
                -- Biographical Info
                TRY_TO_DATE(BIRTHDATE) AS birth_date,
                SCHOOL::VARCHAR AS school,
                COUNTRY::VARCHAR AS country,
                -- Physical Attributes
                HEIGHT::VARCHAR AS height,
                WEIGHT::VARCHAR AS weight,
                -- convert height to inches
                CASE
                        WHEN HEIGHT LIKE '%-%' THEN
                                (SPLIT_PART(HEIGHT, '-', 1)::INTEGER * 12) +
                                SPLIT_PART(HEIGHT, '-', 2)::INTEGER
                        ELSE NULL
                END AS height_inches,
                -- convert weight to integer 
                TRY_CAST(WEIGHT AS INTEGER) AS weight_pounds,
                POSITION::VARCHAR AS position,
                JERSEY::VARCHAR AS jersey_number,
                SEASON_EXP::VARCHAR AS season_experience,
                -- Current team
                TEAM_ID::INTEGER AS current_team_id,
                TEAM_NAME::VARCHAR AS current_team_name,
                TEAM_ABBREVIATION::VARCHAR AS current_team_abbreviation,
                -- Status
                ROSTERSTATUS::VARCHAR AS roster_status,
                -- Career span
                FROM_YEAR::INTEGER AS career_start_year,
                TO_YEAR::INTEGER AS career_end_year,
                -- Draft info
                DRAFT_YEAR::VARCHAR AS draft_year,
                DRAFT_ROUND::VARCHAR AS draft_round,
                DRAFT_NUMBER::VARCHAR AS draft_number,
                -- Metadata
                loaded_at
        FROM source
        WHERE PERSON_ID IS NOT NULL
),

deduped AS(
        SELECT *,
                ROW_NUMBER() OVER(
                        PARTITION BY player_id
                        ORDER BY loaded_at DESC
                        ) AS row_num
        FROM cleaned
)
SELECT * EXCLUDE row_num
FROM deduped
WHERE row_num = 1
