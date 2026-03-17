# First, get the API response structure for table/schema design for the database
from nba_api.stats.endpoints import playergamelogs, commonplayerinfo, commonteamroster
import pandas as pd

# 1. Test PlayerGameLogs
print("PlayerGameLogs Endpoint")
print("-"*50)

game_logs = playergamelogs.PlayerGameLogs(
	season_nullable='2024-25',
	season_type_nullable='Regular Season',
	last_n_games_nullable=5 # small sample for testing
)

# Convert to pandas dataframe
game_logs_df = game_logs.get_data_frames()[0]
print(f"\nColumns: {list(game_logs_df.columns)}")
print(f"\nSample:\n{game_logs_df.head(3)}")
print(f"\nData types:\n{game_logs_df.dtypes}")

# 2. Test CommonPlayerInfo
print("CommonPLayerInfo Endploint")
print("-" * 50)

player_info = commonplayerinfo.CommonPlayerInfo(player_id=1000)
player_info_df = player_info.get_data_frames()[0]
print(f"\nColumns returned: {list(player_info_df.columns)}")
print(f"\nSample:\n{player_info_df.head(3)}")
print(f"\nData types:\n{player_info_df.dtypes}")

# 3. Test CommonTeamRoster
print("CommonTeamRoster Endpoint")
print("-" * 50)

team_roster = commonteamroster.CommonTeamRoster(
	team_id=1000,
	season='2024-25'
)
team_roster_df = team_roster.get_data_frames()[0]
print(f"\nColumns returned: {list(team_roster_df.columns)}")
print(f"\nSample:\n{team_roster_df.head(3)}")
print(f"\nData types:\n{team_roster_df.dtypes}")

print("\n" + "-" * 50)
print("Endpoints verified")
