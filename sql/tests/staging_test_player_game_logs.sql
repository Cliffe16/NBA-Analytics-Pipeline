-- Query the staging view that dbt created
USE SCHEMA STAGING;

SELECT
        game_id,
        game_date,
        player_name,
        is_win,
        minutes_played,
        points,
        field_goals_made,
        field_goals_attempted,
        field_goals_pct,
        total_rebounds,
        assists
FROM stg_player_game_logs
ORDER BY player_name, game_date;
