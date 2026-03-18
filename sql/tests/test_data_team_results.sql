USE DATABASE NBA_ANALYTICS;
USE SCHEMA RAW;
USE WAREHOUSE NBA_WH;

INSERT INTO raw_team_game_results(
	SEASON_ID,
 	TEAM_ID,
 	TEAM_ABBREVIATION,
 	TEAM_NAME,
 	GAME_ID,
 	GAME_DATE,
 	MATCHUP,
 	WL,
 	MIN,
  	PTS, FGM, FGA, FG_PCT,
 	FG3M, FG3A, FG3_PCT,
 	FTM, FTA, FT_PCT,
 	OREB, DREB, REB,
 	AST, STL, BLK, TOV, PF,
 	PLUS_MINUS
) 
VALUES(
	'22024',
	1610612747,
	'LAL',
	'Los Angeles Lakers',
	'0022400001',
	'OCT 22, 2024',
	'LAL vs. DEN',
	'W',
	240,
	110, 40, 85, 0.471,
	12, 35, 0.343,
	18, 22, 0.818,
	10, 35, 45,
	25, 8, 5, 12, 20,
	8
),
(
	'22024',
	1610612743,
	'DEN',
	'Denver Nuggets',
	'0022400001',
	'OCT 22, 2024',
	'DEN @ LAL',
	'L',
	240,
	102, 38, 88, 0.432,
	10, 32, 0.313,
	16, 20, 0.800,
	8, 32, 40,
	22, 6, 4, 15, 22,
	-8
);

-- Verify
SELECT
    TEAM_NAME,
    MATCHUP,
    WL,
    PTS
FROM raw_team_game_results;
