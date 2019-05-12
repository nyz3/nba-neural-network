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

            home_id = game["home_team_id"]
            away_id = game["away_team_id"]

            home_points = game["home_points"]
            home_assists = game["home_assists"]
            home_turnovers = game["home_turnovers"]
            away_points = game["away_points"]
            away_assists = game["away_assists"]
            away_turnovers = game["away_turnovers"]

            # Use current history, winner data, and old stats to add a dict
            # record to ml_data
            winner = game["winner"]
            if home_id in team_season_hist:
                home_games = team_season_hist[home_id]
                home_points_total = sum([i[0] for i in home_games])
                home_assists_total = sum([i[1] for i in home_games])
                home_turnovers_total = sum([i[2] for i in home_games])
                home_points_avg = home_points_total / len(home_games)
                home_assists_avg = home_assists_total / len(home_games)
                home_turnovers_avg = home_turnovers_total / len(home_games)
                home_game_num = len(home_games)
            else:
                home_points_avg = 0
                home_assists_avg = 0
                home_turnovers_avg = 0
                home_game_num = 0

            if away_id in team_season_hist:
                away_games = team_season_hist[away_id]
                away_points_total = sum([i[0] for i in away_games])
                away_assists_total = sum([i[1] for i in away_games])
                away_turnovers_total = sum([i[2] for i in away_games])
                away_points_avg = away_points_total / len(away_games)
                away_assists_avg = away_assists_total / len(away_games)
                away_turnovers_avg = away_turnovers_total / len(away_games)
                away_game_num = len(away_games)
            else:
                away_points_avg = 0
                away_assists_avg = 0
                away_turnovers_avg = 0
                away_game_num = 0

            # Upload to ml stats
            game_ml_data = {
                "home_game_num": home_game_num,
                "away_game_num": away_game_num,
                "game_id": game["game_id"],
                "winner": winner,
                "home_points_avg": home_points_avg,
                "home_assists_avg": home_assists_avg,
                "home_turnovers_avg": home_turnovers_avg,
                "away_points_avg": away_points_avg,
                "away_assists_avg": away_assists_avg,
                "away_turnovers_avg": away_turnovers_avg
            }
            ml_data.append(game_ml_data)

            home_stats = (home_points, home_assists, home_turnovers)
            away_stats = (away_points, away_assists, away_turnovers)
            if home_id in team_season_hist:
                team_season_hist[home_id].append(home_stats)
            else:
                team_season_hist[home_id] = [home_stats]

            if away_id in team_season_hist:
                team_season_hist[away_id].append(away_stats)
            else:
                team_season_hist[away_id] = [away_stats]

    send_to_mongo(ml_data)
    print("Done.")
