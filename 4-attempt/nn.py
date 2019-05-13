import sys
import pymongo
import pprint
import ssl
sys.path.append('../../')
import nba_net.core.networks as net


DB_URL = "mongodb://localhost:27017/"


def filter_data(game_stats):
    if len(game_stats) > 30:
        raise ValueError("This should never happen")
    home_players = []
    away_players = []
    for p in game_stats:
        if game_stats[p]["home"] == 1:
            home_players.append(game_stats[p])
        else:
            away_players.append(game_stats[p])
    hp = sorted(home_players, key=lambda k: k["total_sec"], reverse=True)
    ap = sorted(away_players, key=lambda k: k["total_sec"], reverse=True)

    for pd in hp:
        del pd["home"]
        del pd["total_sec"]
        del pd["player_id"]

    for pd in ap:
        del pd["home"]
        del pd["total_sec"]
        del pd["player_id"]

    # We zero pad our data up to 15 data points
    if len(hp) < 15:
        to_add = 15 - len(hp)
        for i in range(0, to_add):
            hp.append({
                "off_rating": 0,
                "def_rating": 0,
                "usg_pct": 0,
                "plus_minus": 0
            })

    if len(ap) < 15:
        to_add = 15 - len(ap)
        for i in range(0, to_add):
            ap.append({
                "off_rating": 0,
                "def_rating": 0,
                "usg_pct": 0,
                "plus_minus": 0
            })

    home_data = []
    away_data = []

    for pd in hp:
        home_data.append(pd["def_rating"])
        home_data.append(pd["off_rating"])
        home_data.append(pd["plus_minus"])
        home_data.append(pd["usg_pct"])
    for pd in ap:
        away_data.append(pd["def_rating"])
        away_data.append(pd["off_rating"])
        away_data.append(pd["plus_minus"])
        away_data.append(pd["usg_pct"])

    return home_data, away_data


def get_team_stats():
    """Returns a list of all the team-level ML stats for each game, as well as
    the outcome. Each list has the form [0..., 0..., 0..., 0..., ..., 1/0]"""
    client = pymongo.MongoClient(DB_URL)
    db = client.attempt4
    ml_stats = db.learningStats
    parsed_stats = []
    for game_stats in ml_stats.find():
        temp_list = []
        del game_stats["_id"]
        del game_stats["game_id"]
        winner = game_stats["winner"]
        del game_stats["winner"]
        home_player_data, away_player_data = filter_data(game_stats)
        temp_list = temp_list + home_player_data
        temp_list = temp_list + away_player_data
        temp_list.append(winner)
        parsed_stats.append(temp_list)
    return parsed_stats


if __name__ == "__main__":
    team_stats = get_team_stats()
    model = net.OneLayer(120, 60, 1)
    net.train_model(15000, 0.0008, model, team_stats)
