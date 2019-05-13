import requests
import pymongo
import ssl
import time

ENV = 1

# This URL gives us plus_minus for a player in a game
BS_TRAD_URL = \
    "https://stats.nba.com/stats/boxscoretraditionalv2" + \
    "?EndPeriod=0&EndRange=0&GameID={}&RangeType=0&StartPeriod=0&StartRange=0"

# This URL gives us usg_pct, off_rating, def_rating for a player in a game
BS_ADV2_URL = \
    "https://stats.nba.com/stats/boxscoreadvancedv2" + \
    "?EndPeriod=0&EndRange=0&GameID={}&RangeType=0&StartPeriod=0&StartRange=0"


HEADERS = {
    "User-Agent":
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36" +
    " (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36"
}

DB_URL = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "coreData?retryWrites=true"


def get_all_games():
    """Gets all games stored in MongoDB Atlas."""
    client = pymongo.MongoClient(
        DB_URL,
        ssl=True,
        ssl_cert_reqs=ssl.CERT_NONE
    )
    db = client.coreData
    allGames = db.allGames
    return allGames


def get_player_pm(api_url):
    """Gets all the players plus-minus scores from the api_url. Returns a dict
    that contains the stats for each player for that game in particular."""
    json_data = requests.get(api_url, headers=HEADERS).json()
    players_data = json_data["resultSets"][0]["rowSet"]
    results = {}
    for pd in players_data:
        team_id = pd[1]
        team_abbrev = pd[2]
        player_id = pd[4]
        player_name = pd[5]
        start_pos = pd[6]
        minutes = pd[8]
        plus_minus = pd[27]
        results[player_id] = {
            "team_id": team_id,
            "team_abbrev": team_abbrev,
            "player_id": player_id,
            "player_name": player_name,
            "start_pos": start_pos,
            "minutes": minutes,
            "plus_minus": plus_minus
        }
    return results


def get_player_adv(api_url):
    """Gets each player's usg_pct, off_rating, and def_rating from the api_url
    for that game in particular. Returns a list of dictionaries."""
    json_data = requests.get(api_url, headers=HEADERS).json()
    players_data = json_data["resultSets"][0]["rowSet"]
    results = {}
    for pd in players_data:
        player_id = pd[4]
        usg_pct = pd[24]
        off_rating = pd[10]
        def_rating = pd[12]
        results[player_id] = {
            "usg_pct": usg_pct,
            "off_rating": off_rating,
            "def_rating": def_rating
        }
    return results


def call_nba_api(trad_url, adv_url):
    """Staggers calls to NBA's API so we don't get rate limited."""
    players_pm_data = get_player_pm(trad)
    time.sleep(0.4)
    players_adv_data = get_player_adv(adv)
    time.sleep(0.4)
    return (players_pm_data, players_adv_data)


def send_to_mongo(data_list):
    """
    Sends data_list to our MongoDB collection `allPlayerGameStats` in
    db `coreData`. data_list is a list of dictionaries.
    """
    client = pymongo.MongoClient(DB_URL, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
    db = client.coreData
    result = db.allPlayerGameStats.insert_many(data_list)
    return result.inserted_ids


def compute_url_pairs(all_games):
    url_pairs = []
    all_games = all_games.find().sort("game_date", pymongo.DESCENDING)
    for idx, game in enumerate(all_games):
        game_id = game["game_id"]
        game_date = game["game_date"]
        matchup = game["matchup_name"]
        bs_trad = BS_TRAD_URL.format(game_id)
        bs_adv = BS_ADV2_URL.format(game_id)
        url_pairs.append((bs_trad, bs_adv, game_id, game_date, matchup, idx))
    return url_pairs


def partition_list(urls):
    return urls


if __name__ == "__main__":
    all_games = get_all_games()
    url_pairs = compute_url_pairs(all_games)
    url_pairs = partition_list(url_pairs)
    all_player_data = []
    for trad, adv, gid, gd, m, idx in url_pairs:
        print("Processing game", idx + 1, "ID:", gid, "Matchup:", m)
        player_pm_data, player_adv_data = call_nba_api(trad, adv)
        for player_id in player_pm_data:
            adv = player_adv_data[player_id]
            pm = player_pm_data[player_id]
            combo = {**adv, **pm}
            combo["game_id"] = gid
            combo["game_date"] = gd
            combo["matchup_name"] = m
            all_player_data.append(combo)
        if (idx + 1) % 100 == 0:
            print("Sending data to MongoDB")
            send_to_mongo(all_player_data)
            all_player_data = []
        else:
            if idx == len(url_pairs) - 1:
                print("Finished! Sending data to MongoDB")
                send_to_mongo(all_player_data)
                all_player_data = []
