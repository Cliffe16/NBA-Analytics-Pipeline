-- This test checks the shooting percentages in fact_player_stats table
SELECT
    player_fact_key,
    field_goals_made,
    field_goals_attempted,
    field_goals_pct
FROM {{ ref('fact_player_stats') }}
WHERE field_goals_attempted > 0
  -- Check if the calculated percentage deviates from the recorded percentage
  AND ABS((field_goals_made::FLOAT / field_goals_attempted) - field_goals_pct) > 0.01
