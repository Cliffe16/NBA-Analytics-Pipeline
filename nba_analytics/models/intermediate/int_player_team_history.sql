{{ config(
    materialized='ephemeral'
) }}

WITH latest_roster_attributes AS (
    -- Grab position & jersey_number (and team info for players with 0 games played)
    SELECT
        player_id,
        player_name,
        team_id,
        snapshot_date,
        position,
        jersey_number,
        ROW_NUMBER() OVER (
            PARTITION BY player_id, team_id
            ORDER BY snapshot_date DESC
        ) AS rn
    FROM {{ ref('stg_team_rosters') }}
),

game_appearances AS (
    -- Every game a player actually played, giving exact mid-season trade dates
    SELECT
        g.player_id,
        g.player_name,
        g.team_id,
        g.game_date AS activity_date,
        r.position,
        r.jersey_number
    FROM {{ ref('stg_player_game_logs') }} g
    LEFT JOIN latest_roster_attributes r
        ON g.player_id = r.player_id
        AND g.team_id = r.team_id
        AND r.rn = 1
),

roster_only_players AS (
    -- Include rostered players who have not appeared in a game yet
    SELECT
        r.player_id,
        r.player_name,
        r.team_id,
        r.snapshot_date AS activity_date,
        r.position,
        r.jersey_number
    FROM latest_roster_attributes r
    LEFT JOIN (SELECT DISTINCT player_id FROM {{ ref('stg_player_game_logs') }}) g
        ON r.player_id = g.player_id
    WHERE r.rn = 1
      AND g.player_id IS NULL
),

combined_activity AS (
    SELECT * FROM game_appearances
    UNION ALL
    SELECT * FROM roster_only_players
),

team_changes AS (
    SELECT
        player_id,
        player_name,
        team_id,
        activity_date,
        position,
        jersey_number,
        LAG(team_id) OVER (PARTITION BY player_id ORDER BY activity_date) AS previous_team_id
    FROM combined_activity
),

team_change_events AS (
    SELECT
        player_id,
        player_name,
        team_id,
        CASE
            WHEN previous_team_id IS NULL THEN CAST('1990-01-01' AS DATE)
            ELSE activity_date
        END AS effective_start_date,
        position,
        jersey_number
    FROM team_changes
    WHERE previous_team_id IS NULL
       OR team_id != previous_team_id
),

team_end_date AS (
    SELECT
        player_id,
        player_name,
        team_id,
        effective_start_date,
        LEAD(effective_start_date) OVER (PARTITION BY player_id ORDER BY effective_start_date) AS effective_end_date,
        position,
        jersey_number
    FROM team_change_events
)

SELECT
    player_id,
    player_name,
    team_id,
    effective_start_date,
    COALESCE(effective_end_date, CAST('2999-12-31' AS DATE)) AS effective_end_date,
    CASE
        WHEN effective_end_date IS NULL THEN TRUE
        ELSE FALSE
    END AS is_current,
    position,
    jersey_number
FROM team_end_date
