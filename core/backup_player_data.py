import pymongo
import ssl

CORE_DB_URL = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "coreData?retryWrites=true"


def get_all_player_data():
    """Gets the last LOOKBACK_PERIOD games for each player up until the
    `end_date` specified."""
    client = pymongo.MongoClient(
        CORE_DB_URL,
        ssl=True,
        ssl_cert_reqs=ssl.CERT_NONE
    )
    db = client.coreData
    allPlayerGameStats = db.allPlayerGameStats
    return allPlayerGameStats


if __name__ == "__main__":
    player_data = get_all_player_data()
    f = open("player_backup.txt", "w+")
    for idx, pd in enumerate(player_data.find()):
        print("Writing, ", idx)
        log_str = "{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format(
            pd["usg_pct"],
            pd["off_rating"],
            pd["def_rating"],
            pd["team_id"],
            pd["team_abbrev"],
            pd["player_id"],
            pd["player_name"],
            pd["start_pos"],
            pd["minutes"],
            pd["plus_minus"],
            pd["game_id"],
            pd["game_date"],
            pd["matchup_name"]
        )
        f.write(log_str)
    f.close()
