USE DATABASE NBA_ANALYTICS;
USE SCHEMA RAW;
USE WAREHOUSE NBA_WH;

-- Insert player biographical data
INSERT INTO raw_player_info(
	PERSON_ID,
	FIRST_NAME,
	LAST_NAME,
	DISPLAY_FIRST_LAST,
 	DISPLAY_LAST_COMMA_FIRST,
	DISPLAY_FI_LAST,
	BIRTHDATE,
	SCHOOL,
	COUNTRY,
	HEIGHT,
	WEIGHT,
	SEASON_EXP,
	JERSEY,
	POSITION,
	ROSTERSTATUS,
	TEAM_ID,
	TEAM_NAME,
	TEAM_ABBREVIATION,
	FROM_YEAR,
	TO_YEAR,
	DRAFT_YEAR,
	DRAFT_ROUND,
	DRAFT_NUMBER
) 
VALUES(
	2544,
	'LeBron',
	'James',
	'LeBron James',
	'James, LeBron',
	'L. James',
	'1984-12-30',
	'St. Vincent-St. Mary High School',
	'USA',
	'6-9',
	'250',
	21,
	'23',
	'Forward',
	'Active',
	1610612747,
	'Los Angeles Lakers',
	'LAL',
	2003,
	2024,
	'2003',
	'1',
	'1'
),
(
	203076,
	'Anthony',
	'Davis',
	'Anthony Davis',
	'Davis, Anthony',
	'A. Davis',
	'1993-03-11',
	'Kentucky',
	'USA',
	'6-10',
	'253',
	12,
	'3',
	'Forward-Center',
	'Active',
	1610612747,
	'Los Angeles Lakers',
	'LAL',
	2012,
	2024,
	'2012',
	'1',
	'1'
);

-- Verify
SELECT
    DISPLAY_FIRST_LAST,
    HEIGHT,
    WEIGHT,
    POSITION,
    TEAM_ABBREVIATION
FROM raw_player_info;
