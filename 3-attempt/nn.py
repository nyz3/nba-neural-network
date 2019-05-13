import sys
import pymongo
import ssl
sys.path.append('../../')
import nba_net.core.networks as net


# DB_URL = \
#     "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
#     "attempt3?retryWrites=true"

DB_URL = "mongodb://localhost:27017/"


def get_team_stats():
    """Returns a list of all the team-level ML stats for each game, as well as
    the outcome. Each list has the form [0..., 0..., 0..., 0..., ..., 1/0]"""
    # client = pymongo.MongoClient(DB_URL, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
    client = pymongo.MongoClient(DB_URL)
    db = client.attempt3
    ml_stats = db.learningStats
    parsed_stats = []
    for game_stats in ml_stats.find():
        del game_stats["_id"]
        parsed_stats.append(list(game_stats.values()))
    return parsed_stats


if __name__ == "__main__":
    team_stats = get_team_stats()
    model = net.OneLayer(1, 5, 1)
    net.train_model(50000, 0.01, model, team_stats)
