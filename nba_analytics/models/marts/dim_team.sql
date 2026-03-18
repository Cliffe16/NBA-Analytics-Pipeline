{{ config(
    materialized='table'
) }}

WITH team_source AS(
	SELECT DISTINCT
		team_id, -- unique teams
		team_abbreviation,
		team_name
	FROM {{ ref( 'stg_team_game_results') }}
),
team_dimension AS(
	SELECT
		team_id AS team_key, -- Use as surrogate key
		team_id,
		team_abbreviation,
		team_name
	FROM team_source
)

SELECT * FROM team_dimension


