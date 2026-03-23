-- This test ensures we don't have future dates in the game dimension,
-- which could indicate timezone bugs or the API returning scheduled/unplayed games.

SELECT
    game_key,
    game_id,
    game_date
FROM {{ ref('dim_game') }}
WHERE game_date > CURRENT_DATE()
