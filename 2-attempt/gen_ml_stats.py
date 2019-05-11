import pymongo
import ssl
import pprint

DB_URL = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "attempt2?retryWrites=true"

CORE_DB_URL = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "coreData?retryWrites=true"


def get_all_games():
    """Gets all games stored in MongoDB Atlas, sorted by date (old to new)."""
    client = pymongo.MongoClient(
        CORE_DB_URL,
        ssl=True,
        ssl_cert_reqs=ssl.CERT_NONE
    )
    db = client.coreData
    allGames = db.allGames
    return allGames


def get_home_team(game):
    matchup = game["matchup_name"]
    if "@" in matchup.split():
        home_abbrev = matchup.split()[2]
    else:
        home_abbrev = matchup.split()[0]

    team_a_abbrev = game["team_a_team_abbrev"]
    if home_abbrev == team_a_abbrev:
        return 1
    else:
        return 0


def send_to_mongo(ml_stats):
    """
    Sends data_list to our MongoDB collection `learningStats`
    in db `attempt2`. `data_list` is a list of dictionaries.
    """
    client = pymongo.MongoClient(DB_URL, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
    db = client.attempt2
    result = db.learningStats.insert_many(ml_stats)
    return result.inserted_ids


if __name__ == "__main__":
    all_games = get_all_games()
    season_buckets = {}
    sorted_games = all_games.find().sort('game_date', pymongo.ASCENDING)
    for game in sorted_games:
        season = game["season"]
        if season in season_buckets:
            season_buckets[season].append(game)
        else:
            season_buckets[season] = [game]

    # A list of dictionaries we can send direct to attempt2.learningStats
    # Easy upload to MongoDB
    ml_data = []

    for season in season_buckets:
        # What do we do with each season's game in season buckets?
        season_games = season_buckets[season]
        team_season_hist = {}
        for idx, game in enumerate(season_games):

            team_a_id = game["team_a_team_id"]
            team_b_id = game["team_b_team_id"]

            team_a_points = game["team_a_points"]
            team_a_assists = game["team_a_assists"]
            team_a_turnovers = game["team_a_turnovers"]
            team_b_points = game["team_b_points"]
            team_b_assists = game["team_b_assists"]
            team_b_turnovers = game["team_b_turnovers"]

            # Use current history, winner data, and old stats to add a dict
            # record to ml_data
            winner = game["winner"]
            if team_a_id in team_season_hist:
                team_a_games = team_season_hist[team_a_id]
                team_a_points_total = sum([i[0] for i in team_a_games])
                team_a_assists_total = sum([i[1] for i in team_a_games])
                team_a_turnovers_total = sum([i[2] for i in team_a_games])
                team_a_points_avg = team_a_points_total / len(team_a_games)
                team_a_assists_avg = team_a_assists_total / len(team_a_games)
                team_a_turnovers_avg = team_a_turnovers_total / len(team_a_games)
                team_a_game_num = len(team_a_games)
            else:
                team_a_points_avg = 0
                team_a_assists_avg = 0
                team_a_turnovers_avg = 0
                team_a_game_num = 0

            if team_b_id in team_season_hist:
                team_b_games = team_season_hist[team_b_id]
                team_b_points_total = sum([i[0] for i in team_b_games])
                team_b_assists_total = sum([i[1] for i in team_b_games])
                team_b_turnovers_total = sum([i[2] for i in team_b_games])
                team_b_points_avg = team_b_points_total / len(team_b_games)
                team_b_assists_avg = team_b_assists_total / len(team_b_games)
                team_b_turnovers_avg = team_b_turnovers_total / len(team_b_games)
                team_b_game_num = len(team_b_games)
            else:
                team_b_points_avg = 0
                team_b_assists_avg = 0
                team_b_turnovers_avg = 0
                team_b_game_num = 0

            # Upload to ml stats
            game_ml_data = {
                "team_a_game_num": team_a_game_num,
                "team_b_game_num": team_b_game_num,
                "game_id": game["game_id"],
                "winner": winner,
                "team_a_points_avg": team_a_points_avg,
                "team_a_assists_avg": team_a_assists_avg,
                "team_a_turnovers_avg": team_a_turnovers_avg,
                "team_b_points_avg": team_b_points_avg,
                "team_b_assists_avg": team_b_assists_avg,
                "team_b_turnovers_avg": team_b_turnovers_avg,
                "home": get_home_team(game)
            }
            ml_data.append(game_ml_data)

            team_a_stats = (team_a_points, team_a_assists, team_a_turnovers)
            team_b_stats = (team_b_points, team_b_assists, team_b_turnovers)
            if team_a_id in team_season_hist:
                team_season_hist[team_a_id].append(team_a_stats)
            else:
                team_season_hist[team_a_id] = [team_a_stats]

            if team_b_id in team_season_hist:
                team_season_hist[team_b_id].append(team_b_stats)
            else:
                team_season_hist[team_b_id] = [team_b_stats]

    send_to_mongo(ml_data)
    print("Done.")
