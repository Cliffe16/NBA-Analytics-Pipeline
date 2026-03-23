-- This test ensures that a player's effective date ranges in dim_player do not overlap.

WITH player_dates AS (
    SELECT 
        player_id,
        effective_start_date,
        effective_end_date
    FROM {{ ref('dim_player') }}
)

SELECT 
    p1.player_id,
    p1.effective_start_date,
    p1.effective_end_date,
    p2.effective_start_date AS overlapping_start,
    p2.effective_end_date AS overlapping_end
FROM player_dates p1
JOIN player_dates p2 
    ON p1.player_id = p2.player_id
    -- Match different records for the same player
    AND p1.effective_start_date != p2.effective_start_date 
    -- Check for timeline overlap
    AND p1.effective_start_date < p2.effective_end_date
    AND p1.effective_end_date > p2.effective_start_date
