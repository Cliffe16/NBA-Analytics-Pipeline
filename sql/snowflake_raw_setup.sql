USE WAREHOUSE NBA_WH;
USE DATABASE NBA_ANALYTICS;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE raw_player_game_logs (
    -- Surrogate key
    id INTEGER AUTOINCREMENT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- API columns 
    SEASON_YEAR VARCHAR(10),
    PLAYER_ID INTEGER,
    PLAYER_NAME VARCHAR(100),
    NICKNAME VARCHAR(100),
    TEAM_ID INTEGER,
    TEAM_ABBREVIATION VARCHAR(10),
    TEAM_NAME VARCHAR(100),
    GAME_ID VARCHAR(50),
    GAME_DATE VARCHAR(50),
    MATCHUP VARCHAR(100),
    WL VARCHAR(1),

    -- Minutes (integer from API)
    MIN INTEGER,

    -- Field goals
    FGM INTEGER,
    FGA INTEGER,
    FG_PCT FLOAT,

    -- Three pointers
    FG3M INTEGER,
    FG3A INTEGER,
    FG3_PCT FLOAT,

    -- Free throws
    FTM INTEGER,
    FTA INTEGER,
    FT_PCT FLOAT,

    -- Rebounds
    OREB INTEGER,
    DREB INTEGER,
    REB INTEGER,

    -- Other counting stats
    AST INTEGER,
    TOV INTEGER,
    STL INTEGER,
    BLK INTEGER,
    BLKA INTEGER,
    PF INTEGER,
    PFD INTEGER,
    PTS INTEGER,
    PLUS_MINUS INTEGER,

    -- Fantasy/advanced (store but don't use initially)
    NBA_FANTASY_PTS FLOAT,
    DD2 INTEGER,
    TD3 INTEGER,
    WNBA_FANTASY_PTS FLOAT,

    -- Metadata columns
    AVAILABLE_FLAG INTEGER,
    MIN_SEC VARCHAR(10),
    TEAM_COUNT INTEGER,

    -- Raw JSON for full API response
    raw_json VARIANT,

    -- Constraints
    CONSTRAINT pk_raw_player_game_logs PRIMARY KEY (id)
);

-- Add comment for documentation
COMMENT ON TABLE raw_player_game_logs IS
    'Raw player box scores from NBA API PlayerGameLogs endpoint.
     Loaded via Python extraction scripts.
     Column names and data types match API output exactly .';


-----------------------------------------------
--Create Table raw_team_game_results
----------------------------------------------
CREATE OR REPLACE TABLE raw_team_game_results (
    -- Surrogate key
    id INTEGER AUTOINCREMENT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- API columns
    SEASON_ID VARCHAR(10),
    TEAM_ID INTEGER,
    TEAM_ABBREVIATION VARCHAR(10),
    TEAM_NAME VARCHAR(100),
    GAME_ID VARCHAR(50),
    GAME_DATE VARCHAR(50),
    MATCHUP VARCHAR(100),
    WL VARCHAR(1),

    -- Minutes
    MIN INTEGER,

    -- Team stats
    PTS INTEGER,
    FGM INTEGER,
    FGA INTEGER,
    FG_PCT FLOAT,
    FG3M INTEGER,
    FG3A INTEGER,
    FG3_PCT FLOAT,
    FTM INTEGER,
    FTA INTEGER,
    FT_PCT FLOAT,
    OREB INTEGER,
    DREB INTEGER,
    REB INTEGER,
    AST INTEGER,
    STL INTEGER,
    BLK INTEGER,
    TOV INTEGER,
    PF INTEGER,
    PLUS_MINUS INTEGER,

    -- Raw JSON
    raw_json VARIANT,

    CONSTRAINT pk_raw_team_game_results PRIMARY KEY (id)
);

COMMENT ON TABLE raw_team_game_results IS 
    'Raw team game results from NBA API LeagueGameFinder endpoint.
     NOTE: This endpoint can timeout - implement retry logic in the extraction script.';


------------------------------------------
--Create raw_player_info
-----------------------------------------
CREATE OR REPLACE TABLE raw_player_info (
    -- Surrogate key
    id INTEGER AUTOINCREMENT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- API columns 
    PERSON_ID INTEGER,
    FIRST_NAME VARCHAR(50),
    LAST_NAME VARCHAR(50),
    DISPLAY_FIRST_LAST VARCHAR(100),
    DISPLAY_LAST_COMMA_FIRST VARCHAR(100),
    DISPLAY_FI_LAST VARCHAR(100),
    PLAYER_SLUG VARCHAR(100),

    -- Biographical(all strings from API endpoint)
    BIRTHDATE VARCHAR(50),
    SCHOOL VARCHAR(100),
    COUNTRY VARCHAR(50),
    LAST_AFFILIATION VARCHAR(100),
    HEIGHT VARCHAR(10),
    WEIGHT VARCHAR(10),
    SEASON_EXP INTEGER,
    JERSEY VARCHAR(10),
    POSITION VARCHAR(50),

    -- Status
    ROSTERSTATUS VARCHAR(20),
    GAMES_PLAYED_CURRENT_SEASON_FLAG VARCHAR(1),

    -- Current team
    TEAM_ID INTEGER,
    TEAM_NAME VARCHAR(100),
    TEAM_ABBREVIATION VARCHAR(10),
    TEAM_CODE VARCHAR(20),
    TEAM_CITY VARCHAR(50),
    PLAYERCODE VARCHAR(50),

    -- Career span
    FROM_YEAR INTEGER,
    TO_YEAR INTEGER,

    -- Draft info
    DRAFT_YEAR VARCHAR(10),
    DRAFT_ROUND VARCHAR(10),
    DRAFT_NUMBER VARCHAR(10),

    -- Flags
    DLEAGUE_FLAG VARCHAR(1),
    NBA_FLAG VARCHAR(1),
    GAMES_PLAYED_FLAG VARCHAR(1),
    GREATEST_75_FLAG VARCHAR(1),

    -- Raw JSON
    raw_json VARIANT,

    CONSTRAINT pk_raw_player_info PRIMARY KEY (id)
);

COMMENT ON TABLE raw_player_info IS
    'Raw player biographical data from NBA API CommonPlayerInfo endpoint.
     Requires one API call per player - use sparingly or batch with CommonAllPlayers.';


-------------------------------------------
--Create raw_team_rosters
------------------------------------------
CREATE OR REPLACE TABLE raw_team_rosters (
    -- Surrogate key
    id INTEGER AUTOINCREMENT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Snapshot metadata
    snapshot_date DATE,        
    
    -- API columns
    TeamID INTEGER,
    SEASON VARCHAR(10),
    LeagueID VARCHAR(10),
    PLAYER VARCHAR(100),
    NICKNAME VARCHAR(100),
    PLAYER_SLUG VARCHAR(100),
    NUM VARCHAR(10),
    POSITION VARCHAR(50),
    HEIGHT VARCHAR(10),
    WEIGHT VARCHAR(10),
    BIRTH_DATE VARCHAR(50),
    AGE VARCHAR(10),
    EXP VARCHAR(10),
    SCHOOL VARCHAR(100),
    PLAYER_ID INTEGER,
    HOW_ACQUIRED VARCHAR(200),  
    
    -- Raw JSON
    raw_json VARIANT,
    
    CONSTRAINT pk_raw_team_rosters PRIMARY KEY (id)
);

COMMENT ON TABLE raw_team_rosters IS 
    'Raw team roster snapshots from NBA API CommonTeamRoster endpoint.
     Must iterate through all 30 teams (no league-wide endpoint).
     Also detects player trades for SCD Type 2 in dim_player.';

----------------------------------
--Verify Table Creation
----------------------------------
-- Show all schemas
SHOW SCHEMAS;

-- Show tables in RAW schema
USE SCHEMA RAW;
SHOW TABLES;

-- Describe one table to verify structure
DESC TABLE raw_player_game_logs;

-- Check warehouse is created and suspended 
SHOW WAREHOUSES;
