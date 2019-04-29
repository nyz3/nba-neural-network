import pymongo
import ssl

DB_URL = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "attempt1?retryWrites=true"

DB_URL_3 = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "attempt3?retryWrites=true"


def get_all_games():
    """Gets all games stored in MongoDB Atlas."""
    client = pymongo.MongoClient(DB_URL, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
    db = client.attempt1
    allGames = db.allGames
    return allGames


def send_to_mongo(ml_stats):
    """
    Sends data_list to our MongoDB collection `learningStats`
    in db `attempt1`. `data_list` is a list of dictionaries.
    """
    client = pymongo.MongoClient(DB_URL_3, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
    db = client.attempt3
    result = db.learningStats.insert_many(ml_stats)
    return result.inserted_ids


if __name__ == "__main__":
    allGames = get_all_games()
    data = []
    for game in allGames.find():
        matchup_name = game["matchup_name"]
        matchup_parts = matchup_name.split()
        if matchup_parts[1] == "@":
            home_team_abbrev = matchup_parts[2]
            away_team_abbrev = matchup_parts[0]
        else:
            home_team_abbrev = matchup_parts[0]
            away_team_abbrev = matchup_parts[2]
        game_winner = game["winner"]
        team_a_abbrev = game["team_a_team_abbrev"]
        team_b_abbrev = game["team_b_team_abbrev"]
        if team_a_abbrev == home_team_abbrev:
            home_team = "a"
        else:
            home_team = "b"

        if home_team == "a":
            ht = 1
        elif home_team == "b":
            ht = 0
        record = {
            "home_team": ht,
            "winner": game_winner
        }
        data.append(record)
    send_to_mongo(data)
