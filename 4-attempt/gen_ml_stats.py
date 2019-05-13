import pymongo
import ssl
import pprint
import motor.motor_asyncio
import asyncio


CORE_DB_URL = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "coreData?retryWrites=true&ssl=true&ssl_cert_reqs=CERT_NONE"
motor_client = motor.motor_asyncio.AsyncIOMotorClient(CORE_DB_URL)

# Number of previous games to compute the stats for each player for
LOOKBACK_RANGE = 15


async def get_game_players(game_id):
    """Gets all players in the game with id `game_id` and returns a list of
    dictionaries. Each dict contains info for that player (id, name, etc.)"""
    db = motor_client.coreData
    allPlayerGameStats = db.allPlayerGameStats
    return allPlayerGameStats.find({"game_id": game_id})


async def get_raw_players_hist(ids, end_date):
    """Gets the last games for each player up until the `end_date` given."""
    db = motor_client.coreData
    allPlayerGameStats = db.allPlayerGameStats
    query = {
        "player_id": {"$in": ids},
        "game_date": {"$lt": end_date},
        "minutes": {"$ne": None}
    }
    total_hist = allPlayerGameStats.find(query).sort(
        "game_date",
        pymongo.DESCENDING
    )
    return total_hist


async def split_home_away(game_players, home_team_id):
    """Splits the players into their home/away groups."""
    home_players = []
    away_players = []
    async for player in game_players:
        team_id = player["team_id"]
        if home_team_id == team_id:
            home_players.append(player)
        else:
            away_players.append(player)
    return home_players, away_players


async def collect_player_hist(raw_player_hist):
    """Organizes the player histories into a dictionary which maps player ID
    to that respective player's past games (up to LOOKBACK_RANGE)."""
    player_hist = {}
    async for ph in raw_player_hist:
        pid = ph["player_id"]
        if pid in player_hist:
            if len(player_hist[pid]) < LOOKBACK_RANGE:
                player_hist[pid].append(ph)
        else:
            player_hist[pid] = [ph]
    return player_hist


def convert_to_seconds(min_played):
    """Converts the string MM:SS to just an integer that is total seconds."""
    mp = min_played.split(":")
    return int(mp[0]) + int(mp[1])


def aggregate_player_stats(player_ids, home_pids, player_hist):
    player_agg_stats = {}
    for pid in player_ids:
        is_home = 1 if pid in home_pids else 0
        if pid not in player_hist:
            player_agg_stats[pid] = {
                "usg_pct": 0,
                "off_rating": 0,
                "def_rating": 0,
                "plus_minus": 0,
                "home": is_home,
                "total_sec": 0,
                "player_id": pid
            }
        else:
            ph = player_hist[pid]
            num_records = len(ph)
            usg_total = sum([i["usg_pct"] for i in ph])
            or_total = sum([i["off_rating"] for i in ph])
            dr_total = sum([i["def_rating"] for i in ph])
            pm_total = sum([i["plus_minus"] for i in ph])
            total_sec = sum(
                [convert_to_seconds(i["minutes"]) for i in ph]
            )
            player_agg_stats[pid] = {
                "usg_pct": usg_total / num_records,
                "off_rating": or_total / num_records,
                "def_rating": dr_total / num_records,
                "plus_minus": pm_total / num_records,
                "home": is_home,
                "total_sec": total_sec,
                "player_id": pid
            }
    return player_agg_stats


async def send_to_mongo(data_list):
    """
    Sends data_list to our MongoDB collection `allPlayerGameStats` in
    db `coreData`. data_list is a list of dictionaries.
    """
    db = motor_client.attempt4
    result = await db.learningStats.insert_many(data_list)
    return result.inserted_ids


async def process_games():
    db = motor_client.coreData
    all_games = db.allGames
    game_idx = 0
    ml_stats = []
    async for game in all_games.find():
        print("Processing game", game_idx + 1, "Game ID", game["game_id"])
        game_id = game["game_id"]
        home_team_id = game["home_team_id"]
        game_date = game["game_date"]
        raw_game_players = await get_game_players(game_id)
        home_players, away_players = await split_home_away(
            raw_game_players,
            home_team_id
        )
        home_pids = [k["player_id"] for k in home_players]
        away_pids = [k["player_id"] for k in away_players]
        player_ids = home_pids + away_pids
        raw_player_hist = await get_raw_players_hist(player_ids, game_date)
        player_hist = await collect_player_hist(raw_player_hist)
        player_agg_stats = aggregate_player_stats(
            player_ids,
            home_pids,
            player_hist
        )
        final_player_stats = {}
        for idx, pid in enumerate(player_ids):
            new_key = "player_{}".format(idx)
            final_player_stats[new_key] = player_agg_stats[pid]
        final_player_stats["game_id"] = game_id
        final_player_stats["winner"] = game["winner"]
        ml_stats.append(final_player_stats)
        if (game_idx + 1) % 100 == 0:
            print("Sending data to MongoDB")
            await send_to_mongo(ml_stats)
            ml_stats = []
        else:
            if (game_idx + 1) == 12060:
                print("Finished! Sending data to MongoDB")
                await send_to_mongo(ml_stats)
                ml_stats = []
        game_idx += 1

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(process_games())
