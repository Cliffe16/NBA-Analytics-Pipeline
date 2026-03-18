WITH roster_snapshots AS(
	SELECT 
		player_id,
		player_name,
		team_id,
		snapshot_date,
		position,
		jersey_number
	FROM {{ ref('stg_team_rosters') }}
),
-- Detect team changes by comparing the snapshot_date with the previous record
team_changes AS(
	SELECT
		player_id,
                player_name,
                team_id,
                snapshot_date,
                position,
                jersey_number,
		LAG(team_id) OVER(PARTITION BY player_id ORDER BY snapshot_date) AS previous_team_id,
		LAG(snapshot_date) OVER(PARTITION BY player_id ORDER BY snapshot_date) AS previous_snapshot_date
	FROM roster_snapshots
),
-- Flag the first team appearance and the team changes
changes_flagged AS(
	SELECT
		player_id,
                player_name,
                team_id,
                snapshot_date,
                position,
                jersey_number,
		previous_team_id,
		CASE
			WHEN previous_team_id IS NULL THEN TRUE
			WHEN team_id != previous_team_id THEN TRUE 
			ELSE FALSE
		END AS is_team_change
	FROM team_changes
),
-- Keep rows where team changed
team_change_events AS(
	SELECT
		player_id,
                player_name,
                team_id,
                snapshot_date AS effective_start_date,
                position,
                jersey_number
	FROM changes_flagged
	WHERE is_team_change = TRUE
),
-- Add the date of departure
team_end_date AS(
	SELECT
		player_id,
                player_name,
                team_id,
                effective_start_date,
		LEAD(effective_start_date) OVER(PARTITION BY player_id ORDER BY effective_start_date) AS effective_end_date,
                position,
                jersey_number
	FROM team_change_events
)
SELECT
	player_id,
        player_name,
        team_id,
        effective_start_date,
	COALESCE(effective_end_date, '2999-12-31'::DATE) AS effective_end_date,
	CASE
		WHEN effective_end_date IS NULL THEN TRUE
		ELSE FALSE
	END AS is_current,
	position,
	jersey_number
FROM team_end_date

