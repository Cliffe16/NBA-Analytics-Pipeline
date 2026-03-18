WITH team_games AS(
	SELECT
		game_id,
        	game_date,
        	season,
        	team_id,
        	team_abbreviation,
        	home_away,
        	is_win,
        	points,
        	field_goals_made,
        	field_goals_attempted,
        	field_goals_pct,
        	three_pointers_made,
        	three_pointers_attempted,
        	three_point_pct,
        	free_throws_made,
        	free_throws_attempted,
        	free_throw_pct,
        	offensive_rebounds,
        	defensive_rebounds,
        	total_rebounds,
        	assists,
        	steals,
        	blocks,
        	turnovers,
        	personal_fouls
    FROM {{ ref('stg_team_game_results') }}
),
-- Extract home teams
home_teams AS (
	SELECT *
	FROM team_games
	WHERE home_away = 'HOME'
),
-- Extract away teams
away_teams AS (
	SELECT *
	FROM team_games
 	WHERE home_away = 'AWAY'
),
-- Combine the extracted tables
combined AS (
	SELECT
        	h.game_id,
        	h.game_date,
        	h.season,
        	-- Home team
        	h.team_id AS home_team_id,
        	h.team_abbreviation AS home_team_abbr,
        	h.points AS home_points,
        	h.field_goals_made AS home_fgm,
        	h.field_goals_attempted AS home_fga,
        	h.three_pointers_made AS home_fg3m,
        	h.three_pointers_attempted AS home_fg3a,
        	h.free_throws_made AS home_ftm,
        	h.free_throws_attempted AS home_fta,
        	h.total_rebounds AS home_rebounds,
        	h.assists AS home_assists,
        	h.steals AS home_steals,
        	h.blocks AS home_blocks,
        	h.turnovers AS home_turnovers,
        	-- Away team
        	a.team_id AS away_team_id,
        	a.team_abbreviation AS away_team_abbr,
        	a.points AS away_points,
        	a.field_goals_made AS away_fgm,
        	a.field_goals_attempted AS away_fga,
        	a.three_pointers_made AS away_fg3m,
        	a.three_pointers_attempted AS away_fg3a,
        	a.free_throws_made AS away_ftm,
        	a.free_throws_attempted AS away_fta,
        	a.total_rebounds AS away_rebounds,
        	a.assists AS away_assists,
        	a.steals AS away_steals,
        	a.blocks AS away_blocks,
        	a.turnovers AS away_turnovers,
        	-- Derived fields
        	CASE
			WHEN h.is_win THEN h.team_id 
			ELSE a.team_id 
		END AS winning_team_id,
        	ABS(h.points - a.points) AS point_differential,
        	h.points + a.points AS total_points

	FROM home_teams h
		INNER JOIN away_teams a
			ON h.game_id = a.game_id
)

SELECT * FROM combined
