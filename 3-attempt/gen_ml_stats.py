import pymongo
import ssl

CORE_DB_URL = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "coreData?retryWrites=true"

DB_URL = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "attempt3?retryWrites=true"


def get_all_games():
    """Gets all games stored in MongoDB Atlas."""
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
    in db `attempt3`. `data_list` is a list of dictionaries.
    """
    client = pymongo.MongoClient(DB_URL, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
    db = client.attempt3
    result = db.learningStats.insert_many(ml_stats)
    return result.inserted_ids


if __name__ == "__main__":
    allGames = get_all_games()
    data = []
    for game in allGames.find():
        game_winner = game["winner"]
        home_record = {
            "home_team": 1,
            "winner": game_winner
        }
        away_record = {
            "home_team": 0,
            "winner": 0 if game_winner == 1 else 1
        }
        data.append(home_record)
        data.append(away_record)
    send_to_mongo(data)
