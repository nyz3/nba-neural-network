import sys
import pymongo
import ssl
import torch
sys.path.append('../../')
import nba_net.core.networks as net
import pprint

#
# DB_URL = \
#     "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
#     "attempt2?retryWrites=true"

DB_URL = "mongodb://localhost:27017/"


def get_pred_acc(game_ids, model):
    """Gets the model's accuracy on the set of games represented by game_ids"""
    client = pymongo.MongoClient(DB_URL)
    db = client.attempt2
    ml_stats = db.learningStats
    query = {"game_id": {"$in": game_ids}}
    ml_data = ml_stats.find(query)
    parsed_stats = []
    for game_stats in ml_data:
        del game_stats["_id"]
        del game_stats["game_id"]
        winner = game_stats["winner"]
        del game_stats["winner"]
        stats = list(game_stats.values())
        stats.append(winner)
        parsed_stats.append(stats)
    X_data, y_actual = net.Util.split_xy(parsed_stats)
    X_data = torch.tensor(X_data, dtype=torch.float)
    y_actual = torch.tensor(y_actual, dtype=torch.float)
    accuracy = net.Util.accuracy(X_data, y_actual, model)
    return accuracy


def get_stats():
    """Returns a list of all the team-level ML stats for each game, as well as
    the outcome. Each list has the form [0..., 0..., 0..., 0..., ..., 1/0]"""
    # client = pymongo.MongoClient(DB_URL, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
    client = pymongo.MongoClient(DB_URL)
    db = client.attempt2
    ml_stats = db.learningStats
    parsed_stats = []
    for game_stats in ml_stats.find():
        del game_stats["_id"]
        del game_stats["game_id"]
        winner = game_stats["winner"]
        del game_stats["winner"]
        stats = list(game_stats.values())
        stats.append(winner)
        parsed_stats.append(stats)
    return parsed_stats


def exp_train():
    """Exports trained model."""
    team_stats = get_stats()
    model = net.OneLayer(8, 7, 1)
    net.train_model(1750, 0.01, model, team_stats, no_graph=True)
    return model
