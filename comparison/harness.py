import nn_1
import nn_2
import nn_3
import nn_4
import nn_5
import time
import pymongo
import random
import pprint
import statistics
import matplotlib.pyplot as plt

DB_URL = "mongodb://localhost:27017/"


def gen_test_games(num_games):
    """Picks a set of random games from our attempt4 DB for testing all of
    our neural network models."""
    client = pymongo.MongoClient(DB_URL)
    db = client.attempt4
    ml_stats = db.learningStats
    game_stats = list(ml_stats.find())
    return random.sample(game_stats, num_games)


def split_game_halves(game_ids):
    """Divides the list of game_ids into two lists, one for the games which
    occur in the first half of a season, and one for games which occur in
    the second half."""
    client = pymongo.MongoClient(DB_URL)
    db = client.coreData
    ag = db.allGames
    query = {"game_id": {"$in": game_ids}}
    all_games = ag.find(query)
    oct = []
    nov = []
    dec = []
    jan = []
    feb = []
    march = []
    april = []
    for game in all_games:
        game_date = game["game_date"].split("-")
        game_id = game["game_id"]
        month = game_date[1]
        if month == "10":
            oct.append(game_id)
        elif month == "11":
            nov.append(game_id)
        elif month == "12":
            dec.append(game_id)
        elif month == "01":
            jan.append(game_id)
        elif month == "02":
            feb.append(game_id)
        elif month == "03":
            march.append(game_id)
        else:
            april.append(game_id)

    return oct, nov, dec, jan, feb, march, april


if __name__ == "__main__":
    print("Training model #1")
    time.sleep(1.25)
    model_1 = nn_1.exp_train()
    print("Training model #2")
    time.sleep(1.25)
    model_2 = nn_2.exp_train()
    print("Training model #4")
    time.sleep(1.25)
    model_4 = nn_4.exp_train()
    print("Training model #5")
    time.sleep(1.25)
    model_5 = nn_5.exp_train()

    print("------------------")
    all_gains = []
    for k in range(0, 5):
        games = gen_test_games(3000)
        game_ids = [g["game_id"] for g in games]
        oct, nov, dec, jan, feb, march, april = split_game_halves(game_ids)
        # # ml = min(len(oct), len(nov), len(dec), len(jan), len(feb), len(march), len(april))
        # # oct = oct[:ml]
        # # nov = nov[:ml]
        # # dec = dec[:ml]
        # # jan = jan[:ml]
        # # feb = feb[:ml]
        # # march = march[:ml]
        # # april = april[:ml]
        # print(len(oct), len(nov), len(dec), len(jan), len(feb), len(march), len(april))

        oct_acc_1 = nn_1.get_pred_acc(oct, model_1)
        nov_acc_1 = nn_1.get_pred_acc(nov, model_1)
        dec_acc_1 = nn_1.get_pred_acc(dec, model_1)
        jan_acc_1 = nn_1.get_pred_acc(jan, model_1)
        feb_acc_1 = nn_1.get_pred_acc(feb, model_1)
        march_acc_1 = nn_1.get_pred_acc(march, model_1)
        april_acc_1 = nn_1.get_pred_acc(april, model_1)
        res_1 = [oct_acc_1, nov_acc_1, dec_acc_1, jan_acc_1, feb_acc_1, march_acc_1, april_acc_1]
        # print(res_1)
        # print(statistics.stdev(res_1))

        oct_acc_2 = nn_2.get_pred_acc(oct, model_2)
        nov_acc_2 = nn_2.get_pred_acc(nov, model_2)
        dec_acc_2 = nn_2.get_pred_acc(dec, model_2)
        jan_acc_2 = nn_2.get_pred_acc(jan, model_2)
        feb_acc_2 = nn_2.get_pred_acc(feb, model_2)
        march_acc_2 = nn_2.get_pred_acc(march, model_2)
        april_acc_2 = nn_2.get_pred_acc(april, model_2)
        res_2 = [oct_acc_2, nov_acc_2, dec_acc_2, jan_acc_2, feb_acc_2, march_acc_2, april_acc_2]
        # print(res_2)
        # print(statistics.stdev(res_2))

        oct_acc_4 = nn_4.get_pred_acc(oct, model_4)
        nov_acc_4 = nn_4.get_pred_acc(nov, model_4)
        dec_acc_4 = nn_4.get_pred_acc(dec, model_4)
        jan_acc_4 = nn_4.get_pred_acc(jan, model_4)
        feb_acc_4 = nn_4.get_pred_acc(feb, model_4)
        march_acc_4 = nn_4.get_pred_acc(march, model_4)
        april_acc_4 = nn_4.get_pred_acc(april, model_4)
        res_4 = [oct_acc_4, nov_acc_4, dec_acc_4, jan_acc_4, feb_acc_4, march_acc_4, april_acc_4]
        # print(res_4)
        # print(statistics.stdev(res_4))

        oct_acc_5 = nn_5.get_pred_acc(oct, model_5)
        nov_acc_5 = nn_5.get_pred_acc(nov, model_5)
        dec_acc_5 = nn_5.get_pred_acc(dec, model_5)
        jan_acc_5 = nn_5.get_pred_acc(jan, model_5)
        feb_acc_5 = nn_5.get_pred_acc(feb, model_5)
        march_acc_5 = nn_5.get_pred_acc(march, model_5)
        april_acc_5 = nn_5.get_pred_acc(april, model_5)
        res_5 = [oct_acc_5, nov_acc_5, dec_acc_5, jan_acc_5, feb_acc_5, march_acc_5, april_acc_5]
        # print(res_5)
        # print(statistics.stdev(res_5))

        gains = [
            oct_acc_5 - oct_acc_1,
            nov_acc_5 - nov_acc_1,
            dec_acc_5 - dec_acc_1,
            jan_acc_5 - jan_acc_1,
            feb_acc_5 - feb_acc_1,
            march_acc_5 - march_acc_1,
            april_acc_5 - april_acc_1
        ]
        all_gains.append(gains)
        print(gains)
    # Take the average and plot
    avg_gains = [
        sum([i[0] for i in all_gains])/5,
        sum([i[1] for i in all_gains])/5,
        sum([i[2] for i in all_gains])/5,
        sum([i[3] for i in all_gains])/5,
        sum([i[4] for i in all_gains])/5,
    ]
    plt.plot(avg_gains, '-b', label='Average Accuracy Gain (Month to Month)')
    plt.legend(loc="upper left")
    plt.show()
