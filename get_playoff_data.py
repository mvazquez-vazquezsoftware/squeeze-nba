import os
import time
import pandas as pd
import random

from nba_api.stats.static import teams
from nba_api.stats.endpoints.commonteamroster import CommonTeamRoster
from nba_api.stats.endpoints.playergamelog import PlayerGameLog

# Teams remaining in the playoffs
TEAM_NAMES = [
    "Indiana Pacers",
    "Oklahoma City Thunder",
]


# Clean file names
def safe_filename(name):
    return name.replace(" ", "_").replace("-", "_").replace(".", "").replace("'", "")

# Extract player logs for the 2024-25 playoff games.
def extract(player_id: str) -> pd.DataFrame:
    logs_raw = PlayerGameLog(
        player_id=player_id,
        season='2024-25',
        season_type_all_star='Playoffs')
    return logs_raw.get_data_frames()[0]

# Transform `raw_game_logs` by removing unwanted columns.
def transform(player, logs_raw):
    logs_raw['PLAYER'] = player
    logs_raw['PTS+REB+AST'] = logs_raw['PTS'] + logs_raw['REB'] + logs_raw['AST']
    logs_raw['PTS+REB'] = logs_raw['PTS'] + logs_raw['REB']
    logs_raw['PTS+AST'] = logs_raw['PTS'] + logs_raw['AST']
    logs_raw['REB+AST'] = logs_raw['REB'] + logs_raw['AST']
    logs_clean = logs_raw.loc[:, [
        'PLAYER',
        'GAME_DATE',
        'MATCHUP',
        'WL',
        'MIN',
        'PTS',
        'REB',
        'AST',
        'PTS+REB+AST',
        'PTS+REB',
        'PTS+AST',
        'REB+AST',
        'FGM',
        'FGA',
        'FG_PCT',
        'FG3M',
        'FG3A',
        'FG3_PCT',
        'FTM',
        'FTA',
        'FT_PCT',
        'OREB',
        'DREB',
        'STL',
        'BLK',
        'TOV']]
    return logs_clean

def load(player, team_dir, logs_clean):
    if logs_clean.empty:
        return None
    logs_path = os.path.join(team_dir, f"{safe_filename(player)}.csv")
    logs_clean.to_csv(logs_path, index=False)
    return logs_path

# Main function
def main():

    # Get data for every player in the remaining teams.
    for team_name in TEAM_NAMES:

        # Get team roster and make team data directory.
        print(f"\nFetching roster for the {team_name}.")
        team = teams.find_teams_by_full_name(team_name)[0]
        roster = CommonTeamRoster(team_id=team['id']).get_data_frames()[0]
        team_dir = f"data/players/{safe_filename(team['full_name'])}"
        os.makedirs(team_dir, exist_ok=True)
        time.sleep(random.uniform(1, 2))

        # Get game logs for each player on the roster.
        for _, player in roster.iterrows():
            print(f"\tFetching logs for {player['PLAYER']}...", end=' ')
            logs_raw = extract(player['PLAYER_ID'])
            logs_clean = transform(player['PLAYER'], logs_raw)
            logs_path = load(player['PLAYER'], team_dir, logs_clean)
            if logs_path is not None:
                print(f"Saved to {logs_path}")
            else:
                print("No playoff logs found.")
            time.sleep(random.uniform(1, 2))


if __name__ == '__main__':
    main()
