{{ config(
    materialized='table'
) }}

WITH player_team_history AS(
	SELECT
		player_id,
		player_name,
        	team_id,
        	effective_start_date,
        	effective_end_date,
        	is_current,
        	position,
        	jersey_number
	FROM {{ ref('int_player_team_history') }}
),
player_bio AS (
	SELECT
        	player_id,
        	first_name,
        	last_name,
        	birth_date,
        	height_inches,
        	weight_pounds,
        	school,
        	country,
        	draft_year,
        	draft_round,
        	draft_number
    FROM {{ ref('stg_player_info') }}
),
player_dimension AS (
	SELECT
        	-- Surrogate key: player_id + effective_start_date
        	{{ dbt_utils.generate_surrogate_key(['h.player_id', 'h.effective_start_date']) }} AS player_key,
        	-- Natural key
        	h.player_id,
        	-- Player attributes
        	h.player_name,
        	b.first_name,
        	b.last_name,
        	-- Current team 
        	h.team_id,
        	h.position,
        	h.jersey_number,
        	-- Physical attributes 
        	b.birth_date,
        	b.height_inches,
        	b.weight_pounds,
        	-- Background
        	b.school,
        	b.country,
        	b.draft_year,
        	b.draft_round,
        	b.draft_number,
        	-- SCD Type 2 fields
        	h.effective_start_date,
        	h.effective_end_date,
        	h.is_current
	FROM player_team_history h
    		LEFT JOIN player_bio b
        		ON h.player_id = b.player_id
)

SELECT * FROM player_dimension
