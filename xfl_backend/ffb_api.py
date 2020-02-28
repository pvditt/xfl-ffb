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

    results = {"issues_with": []}
    for player_id in players_with_game_stats.keys():
        doc_ref = db.collection(player_id.split("-")[-2]).document(player_id)
        key = "week" + week
        try:
            doc_ref.update({key: players_with_game_stats.get(player_id)})
        except:
            doc_ref.set({key: players_with_game_stats.get(player_id)})
            results.get("issues_with").append(player_id)

    # batch = db.batch()
    # for player_id in players_with_game_stats.keys():
    #     doc_ref = db.collection(player_id.split("-")[-2]).document(player_id)
    #     key = "week" + week
    #     batch.update(doc_ref, {key: players_with_game_stats.get(player_id)})
    # batch.commit()

    return jsonify(results)


def get_defensive_stats(defender):
    defensive_stats = {
        "tackles": defender["Tackles"],
        "sacks": defender["Sacks"],
        "tackles_for_loss": defender["TacklesForLoss"],
        "interceptions": defender["Interceptions"],
        "passes_defender": defender["PassDefensed"],
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
        "first_name": first_name,
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


@app.route('/fetch_fantasy_teams')
def get_league_rosters():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open("XFL Rosters").sheet1
    player_pool = []
    for x in range(7):
        players = sheet.col_values(x + 1)[1:]
        for player in players:
            player_pool.append(player)
    print(player_pool)


@app.route("/fetch_team_stats")
def get_fantasy_stats_for_team():
    return "Need to implement"


@app.route('/')
def main():
    return "Home Page For Hacked Together XFL Fantasy 'app'"


if __name__ == '__main__':
    app.run()
