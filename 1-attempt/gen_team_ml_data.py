import pymongo
import ssl

LOOKBACK_RANGE = 3

DB_URL = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "attempt1?retryWrites=true"


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
    client = pymongo.MongoClient(DB_URL, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
    db = client.attempt1
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
        a_game_stats = {
            "fgm": game["team_a_field_goals_made"],
            "fgm3": game["team_a_three_ptr_made"],
            "fga": game["team_a_field_goals_attempted"],
            "orb": game["team_a_offensive_rebounds"],
            "drb_opp": game["team_b_defensive_rebounds"],
            "tov": game["team_a_turnovers"],
            "fta": game["team_a_free_throws_attempted"]
        }
        b_game_stats = {
            "fgm": game["team_b_field_goals_made"],
            "fgm3": game["team_b_three_ptr_made"],
            "fga": game["team_b_field_goals_attempted"],
            "orb": game["team_b_offensive_rebounds"],
            "drb_opp": game["team_a_defensive_rebounds"],
            "tov": game["team_b_turnovers"],
            "fta": game["team_b_free_throws_attempted"]
        }
        team_a_id = game["team_a_team_id"]
        team_b_id = game["team_b_team_id"]
        if team_a_id in team_game_dict and team_b_id in team_game_dict:
            team_a_hist = team_game_dict[team_a_id]
            team_b_hist = team_game_dict[team_b_id]
            if len(team_a_hist) >= 30 and len(team_b_hist) >= 30:
                a_fgm = sum([a["fgm"] for a in team_a_hist])
                a_fgm3 = sum([a["fgm3"] for a in team_a_hist])
                a_fga = sum([a["fga"] for a in team_a_hist])
                a_orb = sum([a["orb"] for a in team_a_hist])
                a_drb_opp = sum([a["drb_opp"] for a in team_a_hist])
                a_tov = sum([a["tov"] for a in team_a_hist])
                a_fta = sum([a["fta"] for a in team_a_hist])

                b_fgm = sum([b["fgm"] for b in team_b_hist])
                b_fgm3 = sum([b["fgm3"] for b in team_b_hist])
                b_fga = sum([b["fga"] for b in team_b_hist])
                b_orb = sum([b["orb"] for b in team_b_hist])
                b_drb_opp = sum([b["drb_opp"] for b in team_b_hist])
                b_tov = sum([b["tov"] for b in team_b_hist])
                b_fta = sum([b["fta"] for b in team_b_hist])
                ml_stats[game["game_id"]] = {
                    "game_id": game["game_id"],
                    "team_a_efg_pct": eff_fg_pct(a_fgm, a_fgm3, a_fga),
                    "team_a_tov_pct": turnover_pct(a_fga, a_orb, a_tov, a_fta),
                    "team_a_orb_pct": off_reb_pct(a_orb, a_drb_opp),
                    "team_a_ft_pct": free_throw_rate(a_fta, a_fga),
                    "team_b_efg_pct": eff_fg_pct(b_fgm, b_fgm3, b_fga),
                    "team_b_tov_pct": turnover_pct(b_fga, b_orb, b_tov, b_fta),
                    "team_b_orb_pct": off_reb_pct(b_orb, b_drb_opp),
                    "team_b_ft_pct": free_throw_rate(b_fta, b_fga),
                    "winner": game["winner"]
                }
                team_game_dict[game["team_a_team_id"]].pop(0)
                team_game_dict[game["team_b_team_id"]].pop(0)
            team_game_dict[game["team_a_team_id"]].append(a_game_stats)
            team_game_dict[game["team_b_team_id"]].append(b_game_stats)
        elif game["team_a_team_id"] in team_game_dict:
            team_game_dict[game["team_b_team_id"]] = [b_game_stats]
        elif game["team_b_team_id"] in team_game_dict:
            team_game_dict[game["team_a_team_id"]] = [a_game_stats]
        else:
            team_game_dict[game["team_a_team_id"]] = [a_game_stats]
            team_game_dict[game["team_b_team_id"]] = [b_game_stats]
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
