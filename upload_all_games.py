import requests
import pymongo
import ssl

BASE_URL = \
    "https://stats.nba.com/stats/leaguegamelog?Counter=0&DateFrom=&" + \
    "DateTo=&Direction=ASC&LeagueID=00&PlayerOrTeam=T&Season={}" + \
    "&SeasonType=Regular+Season&Sorter=DATE"

HEADERS = {
    "User-Agent":
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36" +
    " (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36"
}
DB_URL = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "nbaData?retryWrites=true"


def get_all_games(season):
    """
    Gets all games for the given season.
    Season is a string of the form: "2018-19", "2017-18", and so on.
    Returns a list of lists (each list contains game data).
    """
    url = BASE_URL.format(season)
    json_data = requests.get(url, headers=HEADERS).json()
    all_games = json_data["resultSets"][0]["rowSet"]
    return all_games


def extract_team_data(game_data):
    """
    Extracts relevant team data from a single game_data array.
    Returns a dictionary of keys to values (like "game_date": "{date}").
    """
    team_id = game_data[1]
    team_abbrev = game_data[2]
    team_name = game_data[3]
    total_minutes_all_players = game_data[8]
    field_goals_made = game_data[9]
    field_goals_attempted = game_data[10]
    three_ptr_made = game_data[12]
    three_ptr_attempted = game_data[13]
    free_throws_made = game_data[15]
    free_throws_attempted = game_data[16]
    offensive_rebounds = game_data[18]
    defensive_rebounds = game_data[19]
    rebounds = game_data[20]
    assists = game_data[21]
    steals = game_data[22]
    blocks = game_data[23]
    turnovers = game_data[24]
    personal_fouls = game_data[25]
    points = game_data[26]
    plus_minus = game_data[27]

    final_dict = {
        "team_id": team_id,
        "team_abbrev": team_abbrev,
        "team_name": team_name,
        "total_minutes_all_players": total_minutes_all_players,
        "field_goals_made": field_goals_made,
        "field_goals_attempted": field_goals_attempted,
        "three_ptr_made": three_ptr_made,
        "three_ptr_attempted": three_ptr_attempted,
        "free_throws_made": free_throws_made,
        "free_throws_attempted": free_throws_attempted,
        "offensive_rebounds": offensive_rebounds,
        "defensive_rebounds": defensive_rebounds,
        "rebounds": rebounds,
        "assists": assists,
        "steals": steals,
        "blocks": blocks,
        "turnovers": turnovers,
        "personal_fouls": personal_fouls,
        "points": points,
        "plus_minus": plus_minus
    }
    return final_dict


def rename_features(team_data, prefix):
    """
    Renames the keys of the team_data dictionary to have the given prefix.
    """
    new_dict = {}
    for key in team_data:
        new_key = prefix + "_" + key
        new_dict[new_key] = team_data[key]
    return new_dict


def compile_games(all_games):
    """
    Takes a list of lists (game data from the NBA API) and returns a dictionary
    of dictionaries that maps game IDs to all the relevant info for that game.
    """
    compiled_games = {}
    for game_data in all_games:
        game_id = game_data[4]
        team_data = extract_team_data(game_data)
        if game_id in compiled_games:
            compiled_games[game_id] = {
                **compiled_games[game_id],
                **rename_features(team_data, "team_b")
            }
        else:
            # game_date, win/loss, matchup all needed exactly once
            compiled_games[game_id] = rename_features(team_data, "team_a")
            winner = 1 if game_data[7] == "W" else 0
            compiled_games[game_id]["winner"] = winner
            compiled_games[game_id]["game_date"] = game_data[5]
            compiled_games[game_id]["game_id"] = game_id
            compiled_games[game_id]["matchup_name"] = game_data[6]

    return compiled_games


def send_to_mongo(data_list):
    """
    Sends data_list to our MongoDB collection `allGames` in db `nbaData
    data_list is a list of dictionaries.
    """
    client = pymongo.MongoClient(DB_URL, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
    db = client.nbaData
    result = db.allGames.insert_many(data_list)
    return result.inserted_ids


if __name__ == "__main__":
    seasons = [
        "2018-19",
        "2017-18",
        "2016-17",
        "2015-16",
        "2014-15",
        "2013-14",
        "2012-13",
        "2011-12",
        "2010-11",
        "2009-10"
    ]
    for season in seasons:
        all_games = get_all_games(season)
        compiled_data = compile_games(all_games)
        data_list = []
        for game_id in compiled_data:
            data_list.append(compiled_data[game_id])
        send_to_mongo(data_list)
        print("Completed processing data for season " + season)
