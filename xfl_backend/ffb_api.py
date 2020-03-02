import json

import gspread
import requests
from bs4 import BeautifulSoup
from firebase_admin import credentials, firestore, initialize_app
from flask import Flask, request, jsonify
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# Initialize Firestore DB
cred = credentials.Certificate("firebase_key.json")
default_app = initialize_app(cred)
db = firestore.client()

# sometimes teams are given as TeamID instead of a mascot name
team_mappings = {
    600: "Renegades",
    601: "Roughnecks",
    602: "Wildcats",
    603: "Dragons",
    604: "Defenders",
    605: "Guardians",
    606: "BattleHawks",
    607: "Vipers"
}


stat_value_mappings = {
    "pass_touchdowns": 4,
    "pass_yards": 0.04,
    "rush_touchdowns": 6,
    "rush_yards": 0.1,
    "receiving_touchdowns": 6,
    "receiving_yards": 0.1,
    "receptions": 0.5,
    "interceptions": -2,
    "one_point_conversions": 1,
    "two_point_conversions": 2,
    "three_point_conversions": 3,
    "forced_fumbles": 2,
    "fumble_recoveries": 2,
    "defensive_interceptions": 4,
    "tackles": 0.2,
    "sacks": 2,
    "tackles_for_loss": 0.5,
    "passes_defended": 0.5,
    "safety": 4,
    "short_fg": 3,
    "medium_fg": 4,
    "long_fg": 5,
    "int_return_tds": 6,
    "fumble_return_tds": 6
}


# TODO: break this method up
@app.route('/load_game_stats')
def load_single_game_stats():
    players_with_game_stats = {}
    home = request.args.get("home")
    away = request.args.get("away")
    week = request.args.get("week")
    stripped_url = request.args.get("url")
    url = "https://" + stripped_url
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html5lib')
    results = soup.find_all('script', {'type': 'text/javascript'})
    issues = {"issues_with": []}
    for script in results:
        js_var = script.string.strip()
        if "offensiveStats" in js_var:
            offensive_stats = json.loads(js_var.split(" = ")[1][:-1])

            for rushing_away in offensive_stats["away"]["rushing"]:
                player_id = get_player_id(rushing_away.get("player"), away)
                if player_id not in players_with_game_stats:
                    players_with_game_stats[player_id] = {}
                running_stats = get_rushing_stats(rushing_away)
                players_with_game_stats.get(player_id).update(running_stats)

            for passing_away in offensive_stats["away"]["passing"]:
                player_id = get_player_id(passing_away.get("player"), away)
                if player_id not in players_with_game_stats:
                    players_with_game_stats[player_id] = {}
                passing_stats = get_passing_stats(passing_away)
                players_with_game_stats.get(player_id).update(passing_stats)

            for receiving_away in offensive_stats["away"]["receiving"]:
                player_id = get_player_id(receiving_away.get("player"), away)
                if player_id not in players_with_game_stats:
                    players_with_game_stats[player_id] = {}
                receiving_stats = get_receiving_stats(receiving_away)
                players_with_game_stats.get(player_id).update(receiving_stats)

            for rushing_home in offensive_stats["home"]["rushing"]:
                player_id = get_player_id(rushing_home.get("player"), home)
                if player_id not in players_with_game_stats:
                    players_with_game_stats[player_id] = {}
                running_stats = get_rushing_stats(rushing_home)
                players_with_game_stats.get(player_id).update(running_stats)

            for passing_home in offensive_stats["home"]["passing"]:
                player_id = get_player_id(passing_home.get("player"), home)
                if player_id not in players_with_game_stats:
                    players_with_game_stats[player_id] = {}
                passing_stats = get_passing_stats(passing_home)
                players_with_game_stats.get(player_id).update(passing_stats)

            for receiving_home in offensive_stats["home"]["receiving"]:
                player_id = get_player_id(receiving_home.get("player"), home)
                if player_id not in players_with_game_stats:
                    players_with_game_stats[player_id] = {}
                receiving_stats = get_receiving_stats(receiving_home)
                players_with_game_stats.get(player_id).update(receiving_stats)
        if "defensiveStats" in js_var:
            defensive_game_stats = json.loads(js_var.split(" = ")[1][:-1])

            for away_defender in defensive_game_stats["away"]["defensive"]:
                player_id = get_player_id(away_defender["player"], away)
                if player_id not in players_with_game_stats:
                    players_with_game_stats[player_id] = {}
                defensive_stats = get_defensive_stats(away_defender)
                players_with_game_stats.get(player_id).update(defensive_stats)

            for home_defender in defensive_game_stats["home"]["defensive"]:
                player_id = get_player_id(home_defender["player"], home)
                if player_id not in players_with_game_stats:
                    players_with_game_stats[player_id] = {}
                defensive_stats = get_defensive_stats(home_defender)
                players_with_game_stats.get(player_id).update(defensive_stats)

        if "playList" in js_var:
            possessions = json.loads(js_var.split(" = ")[1][:-1])
            for play in possessions["plays"]:
                if play["IsScoringPlay"]:
                    if play["playScoreType"] != "Touchdown":
                        docs = list(db.collection(team_mappings.get(play["playScoringTeamId"])).where(
                            "first_name", "array_contains", play["playScorer"].split()[0]).where(
                            "last_name", "==", play["playScorer"].split()[1]).stream())
                        if len(docs) != 1:
                            issues.get("issues_with").append(play["playScorer"] + "-" + team_mappings.get(play["playScoringTeamId"]))
                        else:
                            player_id = docs[0].to_dict().get("player_id")
                            if player_id not in players_with_game_stats:
                                players_with_game_stats[player_id] = {}
                            if play["playScoreType"] == "Fumble":
                                fumble_return_tds = players_with_game_stats.get("fumble_return_tds", 0)
                                players_with_game_stats.get(player_id).update({"fumble_return_tds": fumble_return_tds + 1})
                            elif play["playScoreType"] == "Intercept":
                                int_return_tds = players_with_game_stats.get("int_return_tds", 0)
                                players_with_game_stats.get(player_id).update({"int_return_tds": int_return_tds + 1})
                            elif play["playScoreType"] == "Field Goal":
                                if "field_goals" not in players_with_game_stats.get(player_id):
                                    players_with_game_stats.get(player_id)["field_goals"] = []
                                fg_distance = play["ShortPlayDescription"].split()[0]
                                players_with_game_stats.get(player_id).get("field_goals").append(fg_distance)
                            elif play["playScoreType"] == "One Point Successful Conversion":
                                one_point_conversions = players_with_game_stats.get("one_point_conversions", 0)
                                players_with_game_stats.get(player_id).update({"one_point_conversions": one_point_conversions + 1})
                            elif play["playScoreType"] == "Two Point Successful Conversion":
                                two_point_conversions = players_with_game_stats.get("two_point_conversions", 0)
                                players_with_game_stats.get(player_id).update({"two_point_conversions": two_point_conversions + 1})
                            elif play["playScoreType"] == "Three Point Successful Conversion":
                                three_point_conversions = players_with_game_stats.get("three_point_conversions", 0)
                                players_with_game_stats.get(player_id).update({"three_point_conversions": three_point_conversions + 1})

    for player_id in players_with_game_stats.keys():
        doc_ref = db.collection(player_id.split("-")[-2]).document(player_id)
        key = "week" + week
        try:
            doc_ref.update({key: players_with_game_stats.get(player_id)})
        except:
            doc_ref.set({key: players_with_game_stats.get(player_id)})
            issues.get("issues_with").append(player_id)

    # batch = db.batch()
    # for player_id in players_with_game_stats.keys():
    #     doc_ref = db.collection(player_id.split("-")[-2]).document(player_id)
    #     key = "week" + week
    #     batch.update(doc_ref, {key: players_with_game_stats.get(player_id)})
    # batch.commit()

    return jsonify(issues)


def get_defensive_stats(defender):
    defensive_stats = {
        "tackles": defender["Tackles"],
        "sacks": defender["Sacks"],
        "tackles_for_loss": defender["TacklesForLoss"],
        "defensive_interceptions": defender["Interceptions"],
        "passes_defended": defender["PassDefensed"],
        "forced_fumbles": defender["ForcedFumbles"],
        "fumble_recoveries": defender["FumbleRecoveries"],
        "safety": defender["safety"],
    }
    return defensive_stats


def get_receiving_stats(receiving):
    receiving_stats = {
        "receiving_yards": receiving.get("yards"),
        "receiving_touchdowns": receiving.get("touchdowns"),
        "receptions": receiving.get("receptions")
    }
    return receiving_stats


def get_passing_stats(passing):
    passing_stats = {
        "pass_yards": passing.get("yards"),
        "pass_touchdowns": passing.get("touchdowns"),
        "interceptions": passing.get("interceptions")
    }
    return passing_stats


def get_rushing_stats(rushing):
    running_stats = {
        "rush_yards": rushing.get("yards"),
        "rush_touchdowns": rushing.get("touchdowns")
    }
    return running_stats


def get_player_id(player, team):
    name = player.get("displayName")
    number = player.get("jerseyNumber")
    player_id = name + "-" + team + "-" + number
    return player_id


# ran to populate db initially
def upload_xfl_rosters():
    mascot_names = []
    players = []
    url = "https://www.sportingnews.com/us/nfl/" \
          "news/xfl-rosters-2020-here-are-the-52-man-rosters-for-all-eight-teams/gdlifutwxnsv1x41iejvex9h1"
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html5lib')
    team_names = soup.find_all(class_="block-element__h2")
    for i, mascot in enumerate(team_names):
        mascot_names.append(mascot.text.split()[-2])
    team_rosters = soup.findAll("div", class_="content-element__table-container")
    for i, team in enumerate(team_rosters):
        table = team.find('table')
        table_body = table.find('tbody')
        rows = table_body.find_all('tr')
        for j, row in enumerate(rows):
            if j > 0:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                player = [ele for ele in cols if ele]
                player.append(mascot_names[i])
                players.append(player)
    for player in players:
        team = player[-1]
        jersey_number = player[0]
        if len(player) == 8:
            position = player[3]
            if player[-1] == 'Wildcats':
                first_name = player[2]
                last_name = player[1].split()[0]
            else:
                first_name = player[1]
                last_name = player[2].split()[0]
        elif "," in player[1]:
            position = player[2]
            first_name = player[1].split(", ")[1]
            last_name = player[1].split(", ")[0].split()[0]
        else:
            position = player[2]
            first_name = player[1].split()[0]
            last_name = player[1].split()[1].split()[0]

        create_player(first_name, last_name, position, jersey_number, team)
    return "loaded rosters"


@app.route('/create_player')
def create_player_route():
    first_name = request.args.get("first_name")
    last_name = request.args.get("last_name")
    position = request.args.get("last_name")
    team = request.args.get("team")
    jersey_number = request.args.get("jersey_number")
    if first_name is None or last_name is None or position is None or team is None or jersey_number is None:
        return "need more values to create a player"
    create_player(first_name, last_name, position, jersey_number, team)
    return "created a player"


@app.route('/retrieve_player')
def retrieve_player_route():
    player_id = request.args.get("player_id")
    week = request.args.get("week")
    if player_id is None or week is None:
        return "please pass in player_id to get data for"
    weekly_stats = retrieve_player(player_id, week)
    return jsonify(weekly_stats)


@app.route('/update_player')
def update_player_route():
    player_id = request.args.get("player_id")
    key = request.args.get("key")
    value = request.args.get("value")
    if player_id is None or key is None or value is None:
        return "none of player_id, key, nor value can be null"
    update_player(player_id, key, value)
    return "updated player"


@app.route('/delete_player')
def delete_player_route():
    player_id = request.args.get("player_id")
    if player_id is None:
        return "please pass in player_id to be deleted"
    delete_player(player_id)
    return "deleted player"


def create_player(first_name, last_name, position, jersey_number, team):
    player_id = first_name[0] + "." + last_name + "-" + team + "-" + jersey_number
    player_meta = {
        "player_id": player_id,
        "first_name": [first_name],
        "last_name": last_name,
        "position": position,
        "jersey_number": jersey_number,
        "team": team
    }
    try:
        db.collection(player_id.split("-")[-2]).document(player_id).set(player_meta)
    except:
        print("Issue creating player with Id key: " + player_id)


def retrieve_player(player_id, week):
    try:
        result = db.collection(player_id.split("-")[-2]).document(player_id).get().to_dict().get("week" + week)
    except:
        result = "Issue loading player ID: " + player_id + "for week " + week
    return result


def update_player(player_id, key, value):
    try:
        db.collection(player_id.split("-")[-2]).document(player_id).update({
            key: value
        })
    except:
        print("Issue updating key: " + key + "for " + player_id)


def delete_player(player_id):
    try:
        db.collection(player_id.split("-")[-2]).document(player_id).delete()
    except:
        print("Issue deleting player with id: " + player_id)


@app.route('/get_teams_scores')
def get_team_scores():
    week_number = request.args.get("week")
    teams = get_league_rosters()
    team_stats = {}
    for team, roster in teams.items():
        team_stats[team] = {}
        team_total_points = 0
        for player_id in roster:
            player = db.collection(player_id.split("-")[-2]).document(player_id).get().to_dict()
            player_stats = player.get("week" + week_number)
            if player_stats is None:
                team_stats[team][player_id] = {"total_points": 0}
            else:
                player_points = get_player_points_total(player_stats, player.get("position") == "TE")
                team_total_points += player_points
                player_stats["player_points_total"] = player_points
                team_stats[team][player_id] = player_stats
        team_stats[team]["team_total_points"] = round(team_total_points, 2)
    return jsonify(team_stats)


def get_player_points_total(player_stats, multiplier):
    total_points = 0
    for stat, value in player_stats.items():
        if stat == "field_goals":
            for fg in value:
                if int(fg) >= 50:
                    total_points += stat_value_mappings.get("long_fg")
                elif int(fg) >= 40:
                    total_points += stat_value_mappings.get("medium_fg")
                else:
                    total_points += stat_value_mappings.get("short_fg")
        else:
            total_points += stat_value_mappings.get(stat) * value
    if multiplier:
        return round(total_points * 1.5, 2)
    return round(total_points, 2)

def get_league_rosters():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open("XFL Rosters").sheet1
    teams = {}
    for x in range(5):
        team = sheet.col_values(x + 1)[0]
        teams[team] = []
        players = sheet.col_values(x + 1)[1:]
        for player in players:
            teams[team].append(player)
    return teams


@app.route("/fetch_team_stats")
def get_fantasy_stats_for_team():
    return "Need to implement"


@app.route('/')
def main():
    return "Home Page For Hacked Together XFL Fantasy 'app'"


if __name__ == '__main__':
    app.run()
