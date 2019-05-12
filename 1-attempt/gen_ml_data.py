import pymongo
import ssl

LOOKBACK_RANGE = 5

DB_URL = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "attempt1?retryWrites=true"

CORE_DB_URL = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "coreData?retryWrites=true"


def eff_fg_pct(fgm, fgm3, fga):
    """Computes the effective field goal percentage given fgm, fgm3, fga.
    fgm is field goals made, fgm3 is 3-ptr field goals made, and fga is
    field goals attempted."""
    weight = fgm + (0.5 * fgm3)
    return (weight/fga)


def turnover_pct(fga, off_reb, turnovers, fta):
    """Computes the turnover percentage.
    fga is field goals attempted, off_reb is offensive rebounds, turnovers
    is the number of turnovers, and fta is free throws attempted."""
    y = 0.44
    possessions = (fga - off_reb) + turnovers + (y * fta)
    return (turnovers/possessions)


def off_reb_pct(off_reb, def_reb_opp):
    """Computes the offensive rebound percentage.
    off_reb is offensive rebounds and def_reb_opp is the number of defensive
    rebounds by the opponent."""
    return (off_reb/(off_reb + def_reb_opp))


def free_throw_rate(fta, fga):
    """Computes the free throw rate. fta is free throws attempted,
    fga is field goals attempted."""
    return (fta/fga)


def get_all_games():
    """Gets all games stored in MongoDB Atlas."""
    client = pymongo.MongoClient(
        CORE_DB_URL,
        ssl=True,
        ssl_cert_reqs=ssl.CERT_NONE
    )
    db = client.coreData
    allGames = db.allGames
    return allGames


def compute_ml_stats(allGames):
    """Returns a list of dict of stats that we can use for training our ANN.
    Each dictionary has the following form: game_id: {game_id, stats, outcome}.
    The list of dicts is suitable for direct upload to MongoDB."""
    team_game_dict = {}
    ml_stats = {}
    idx = 0
    for game in allGames.find():
        home_game_stats = {
            "fgm": game["home_field_goals_made"],
            "fgm3": game["home_three_ptr_made"],
            "fga": game["home_field_goals_attempted"],
            "orb": game["home_offensive_rebounds"],
            "drb_opp": game["away_defensive_rebounds"],
            "tov": game["home_turnovers"],
            "fta": game["home_free_throws_attempted"]
        }
        away_game_stats = {
            "fgm": game["away_field_goals_made"],
            "fgm3": game["away_three_ptr_made"],
            "fga": game["away_field_goals_attempted"],
            "orb": game["away_offensive_rebounds"],
            "drb_opp": game["home_defensive_rebounds"],
            "tov": game["away_turnovers"],
            "fta": game["away_free_throws_attempted"]
        }
        home_id = game["home_team_id"]
        away_id = game["away_team_id"]
        if home_id in team_game_dict and away_id in team_game_dict:
            home_hist = team_game_dict[home_id]
            away_hist = team_game_dict[away_id]
            if len(home_hist) >= LOOKBACK_RANGE and \
               len(away_hist) >= LOOKBACK_RANGE:
                home_fgm = sum([a["fgm"] for a in home_hist])
                home_fgm3 = sum([a["fgm3"] for a in home_hist])
                home_fga = sum([a["fga"] for a in home_hist])
                home_orb = sum([a["orb"] for a in home_hist])
                home_drb_opp = sum([a["drb_opp"] for a in home_hist])
                home_tov = sum([a["tov"] for a in home_hist])
                home_fta = sum([a["fta"] for a in home_hist])

                away_fgm = sum([b["fgm"] for b in away_hist])
                away_fgm3 = sum([b["fgm3"] for b in away_hist])
                away_fga = sum([b["fga"] for b in away_hist])
                away_orb = sum([b["orb"] for b in away_hist])
                away_drb_opp = sum([b["drb_opp"] for b in away_hist])
                away_tov = sum([b["tov"] for b in away_hist])
                away_fta = sum([b["fta"] for b in away_hist])
                ml_stats[game["game_id"]] = {
                    "game_id": game["game_id"],
                    "home_efg_pct": eff_fg_pct(home_fgm, home_fgm3, home_fga),
                    "home_tov_pct": turnover_pct(home_fga, home_orb, home_tov, home_fta),
                    "home_orb_pct": off_reb_pct(home_orb, home_drb_opp),
                    "home_ft_pct": free_throw_rate(home_fta, home_fga),
                    "away_efg_pct": eff_fg_pct(away_fgm, away_fgm3, away_fga),
                    "away_tov_pct": turnover_pct(away_fga, away_orb, away_tov, away_fta),
                    "away_orb_pct": off_reb_pct(away_orb, away_drb_opp),
                    "away_ft_pct": free_throw_rate(away_fta, away_fga),
                    "winner": game["winner"]
                }
                team_game_dict[game["home_team_id"]].pop(0)
                team_game_dict[game["away_team_id"]].pop(0)
            team_game_dict[game["home_team_id"]].append(home_game_stats)
            team_game_dict[game["away_team_id"]].append(away_game_stats)
        elif game["home_team_id"] in team_game_dict:
            team_game_dict[game["away_team_id"]] = [away_game_stats]
        elif game["away_team_id"] in team_game_dict:
            team_game_dict[game["home_team_id"]] = [home_game_stats]
        else:
            team_game_dict[game["home_team_id"]] = [home_game_stats]
            team_game_dict[game["away_team_id"]] = [away_game_stats]
        idx += 1
        print("Processed game " + str(idx))
    return list(ml_stats.values())


def send_to_mongo(ml_stats):
    """
    Sends data_list to our MongoDB collection `learningStats`
    in db `attempt1`. `data_list` is a list of dictionaries.
    """
    client = pymongo.MongoClient(DB_URL, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
    db = client.attempt1
    result = db.learningStats.insert_many(ml_stats)
    return result.inserted_ids


if __name__ == "__main__":
    allGames = get_all_games()
    ml_stats = compute_ml_stats(allGames)
    send_to_mongo(ml_stats)
    print("Done.")
