import sys
import pymongo
import ssl
sys.path.append('../../')
import nba_net.core.networks as net


DB_URL = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "attempt2?retryWrites=true"


def get_stats():
    """Returns a list of all the team-level ML stats for each game, as well as
    the outcome. Each list has the form [0..., 0..., 0..., 0..., ..., 1/0]"""
    client = pymongo.MongoClient(DB_URL, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
    db = client.attempt2
    ml_stats = db.learningStats
    parsed_stats = []
    for game_stats in ml_stats.find():
        del game_stats["_id"]
        del game_stats["game_id"]
        parsed_stats.append(list(game_stats.values()))
    return parsed_stats


if __name__ == "__main__":
    team_stats = get_stats()
    model = net.OneLayer(9, 7, 1)
    net.train_model(30000, 0.01, model, team_stats)
