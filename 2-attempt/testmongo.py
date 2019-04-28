import pymongo 

DB_URL = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "attempt1?retryWrites=true"


def send_to_mongo(ml_stats):
    """
    Sends data_list to our MongoDB collection `learningStats`
    in db `attempt1`. `data_list` is a list of dictionaries.
    """
    client = pymongo.MongoClient(DB_URL, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
    db = client.attempt1
    result = db.learningStats.insert_many(ml_stats)
    return result.inserted_ids

def get_all_games():
    """Gets all games stored in MongoDB Atlas."""
    client = pymongo.MongoClient(DB_URL, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
    db = client.attempt1
    allGames = db.allGames
    return allGames

if __name__ == "__main__":
    allGames = get_all_games()
    ml_stats = compute_ml_stats(allGames)
    send_to_mongo(ml_stats)
    print("Done.")
