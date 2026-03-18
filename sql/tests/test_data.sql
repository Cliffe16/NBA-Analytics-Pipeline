USE DATABASE NBA_ANALYTICS;
USE SCHEMA RAW;
USE WAREHOUSE NBA_WH;

-- Insert dummy data
INSERT INTO raw_player_game_logs(
	SEASON_YEAR,
	PLAYER_ID,
	PLAYER_NAME,
	TEAM_ID,
	TEAM_ABBREVIATION,
	TEAM_NAME,
 	GAME_ID,
 	GAME_DATE,
 	MATCHUP,
 	WL,
 	MIN,
 	FGM, FGA, FG_PCT,
 	FG3M, FG3A, FG3_PCT,
 	FTM, FTA, FT_PCT,
 	OREB, DREB, REB,
 	AST, STL, BLK, TOV, PF,
 	PTS,
 	PLUS_MINUS
)
VALUES(
	'2024-25',
	2544,
	'LeBron James',
	1610612747,
	'LAL',
	'Los Angeles Lakers',
	'0022400001',
	'OCT 22, 2024',
	'LAL vs. DEN',
	'W',
	35,
	10, 20, 0.500,
	2, 6, 0.333,
	3, 4, 0.750,
 	2, 6, 8,
 	7, 1, 1, 3, 2,
 	25,
 	12
),
(
	'2024-25',
	2544,
	'LeBron James',
	1610612747,
	'LAL',
	'Los Angeles Lakers',
	'0022400002',
	'OCT 25, 2024',
	'LAL @ PHX',
	'L',
	38,
	12, 22, 0.545,
	3, 8, 0.375,
	4, 5, 0.800,
	1, 8, 9,
	10, 2, 0, 2, 3,
	31,
	-5
),
(
	'2024-25',
	203076,
	'Anthony Davis',
	1610612747,
	'LAL',
	'Los Angeles Lakers',
	'0022400001',
	'OCT 22, 2024',
	'LAL vs. DEN',
	'W',
	33,
	9, 15, 0.600,
	0, 1, 0.000,
	6, 8, 0.750,
	4, 8, 12,
	3, 1, 3, 1, 2,
	24,
	15
);
-- Verify the data was inserted
SELECT
	PLAYER_NAME,
	GAME_DATE,
	PTS,
	REB,
	AST,
	loaded_at
FROM raw_player_game_logs
ORDER BY loaded_at DESC;
