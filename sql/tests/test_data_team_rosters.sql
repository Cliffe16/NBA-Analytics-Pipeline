USE DATABASE NBA_ANALYTICS;
USE SCHEMA RAW;
USE WAREHOUSE NBA_WH;

-- Insert dummy roster snapshots 
INSERT INTO raw_team_rosters(
	snapshot_date,
	TeamID,
	SEASON,
	LeagueID,
	PLAYER,
	PLAYER_ID,
	NUM,
	POSITION,
	HEIGHT,
	WEIGHT,
	BIRTH_DATE,
	AGE,
	EXP,
	SCHOOL,
	HOW_ACQUIRED
) 
VALUES(
	'2024-10-22',
	1610612747,
	'2024-25',
	'00',
	'LeBron James',
	2544,
	'23',
	'Forward',
 	'6-9',
	'250',
	'DEC 30, 1984',
	'39',
	'21',
	'St. Vincent-St. Mary HS (OH)',
	'Trade'
),
(
	'2024-10-22',
	1610612747,
	'2024-25',
	'00',
	'Anthony Davis',
	203076,
	'3',
	'Forward-Center',
	'6-10',
	'253',
	'MAR 11, 1993',
	'31',
	'12',
	'Kentucky',
	'Trade'
);

-- Verify
SELECT
	snapshot_date,
	PLAYER,
	POSITION,
 	HEIGHT,
 	WEIGHT
FROM raw_team_rosters;
