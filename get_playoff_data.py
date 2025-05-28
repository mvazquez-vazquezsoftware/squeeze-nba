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
    "Minnesota Timberwolves",
    "New York Knicks",
    "Oklahoma City Thunder",
]


# Clean file names
def safe_filename(name):
    return name.replace(" ", "_").replace("-", "_").replace(".", "").replace("'", "")


# Main function
def main():

    # Get data for every player in the remaining teams.
    for team_name in TEAM_NAMES:
        print(f"\nFetching roster for the {team_name}.")

        # Get team roster.
        team = teams.find_teams_by_full_name(team_name)[0]
        roster = CommonTeamRoster(team_id=team['id']).get_data_frames()[0]
        time.sleep(random.uniform(1, 2))

        # Make team directory.
        team_folder = f"data/players/{safe_filename(team['full_name'])}"
        os.makedirs(team_folder, exist_ok=True)

        # Get game logs for each player on the roster.
        for _, player in roster.iterrows():
            player_name = player['PLAYER']
            print(f"\tFetching logs for {player_name}...", end=' ')

            # Get player logs.
            logs = PlayerGameLog(
                player_id=player['PLAYER_ID'],
                season='2024-25',
                season_type_all_star='Playoffs'
            ).get_data_frames()[0]
            time.sleep(random.uniform(1, 2))

            # Save player logs as csv.
            if not logs.empty:
                logs['PLAYER_NAME'] = player_name
                filename = f"{safe_filename(player_name)}.csv"
                logs.to_csv(os.path.join(team_folder, filename), index=False)
                print(f"Saved to {team_folder}/{filename}.")
            else:
                print("No playoff logs found.")


if __name__ == '__main__':
    main()
