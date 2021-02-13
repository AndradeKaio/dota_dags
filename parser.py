from psycopg2.extras import Json
from datetime import datetime
from dotenv import load_dotenv
import os
import json
import requests
import psycopg2 as pg
import fields, query

load_dotenv()

def get_matches(mmr=None):
    params = {'mmr_ascending': mmr}
    request = requests.get(
        'https://api.opendota.com/api/publicMatches',
        params=params,
    )
    if request.status_code == 200:
        return json.loads(request.content)

def get_match(match_id):
    request = requests.get(
        'https://api.opendota.com/api/matches/{}'.format(match_id)
    )
    if request.status_code == 200:
        return json.loads(request.content)



def get_matches_mock():
    with open('matches.json', 'rt') as file:
        data = file.read()
        file.close()
    return json.loads(data)


def get_matches_info_mock():
    with open('matches_infos.json', 'rt') as file:
        data = file.read()
        file.close()
    return json.loads(data)

import fields


def transform(data: dict):
    matches = data.get('matches')
    matches_info = data.get('matches_info')

    _matches_info = []
    _matches = []
    players = []

    for match in matches:
        if match:
            _matches.append(
                {fv: match[fk] if fk in match.keys() else None for fk, fv in fields.matches_fields.items()}
            )

    for match in matches_info:
        match['start_time'] = datetime.fromtimestamp(match['start_time'])
        match['draft_timings'] = [Json(match['draft_timings'])]


        if match:
            _players = []
            match_id = match.get('match_id')
            _matches_info.append({
                fv: match[fk] if fk in match.keys() else None for fk, fv in fields.match_info_fields.items()
            })

            for player in match.get('players'):
                player['permanent_buffs'] = [Json(player['permanent_buffs'])]
                benchmark_fields = player.get('benchmarks')
                benchmark_fields['gold_per_min'] = benchmark_fields['gold_per_min'].get('raw')
                benchmark_fields['xp_per_min'] = benchmark_fields['xp_per_min'].get('raw')
                benchmark_fields['kills_per_min'] = benchmark_fields['kills_per_min'].get('raw')
                benchmark_fields['last_hits_per_min'] = benchmark_fields['last_hits_per_min'].get('raw')
                benchmark_fields['hero_damage_per_min'] = benchmark_fields['hero_damage_per_min'].get('raw')
                benchmark_fields['hero_healing_per_min'] = benchmark_fields['hero_healing_per_min'].get('raw')
                benchmark_fields['tower_damage'] = benchmark_fields['tower_damage'].get('raw')
                benchmark_fields['stuns_per_min'] = benchmark_fields['stuns_per_min'].get('raw')
                _benchmark_fields = {
                    fv: benchmark_fields[fk] if benchmark_fields.get(fk) else None for fk, fv in fields.benchmark_fields.items()
                }
                player_fields = {
                    fv: player[fk] if player.get(fk) else None for fk, fv in fields.player_fields.items()
                }
                players.append({'match_id': match_id, **player_fields, **_benchmark_fields})

    return {'matches': _matches, 'matches_info': _matches_info, 'players': players}



def load(data: dict):
    matches = data.get('matches')
    matches_info = data.get('matches_info')
    players = data.get('players')

    try:
        dbconnect = pg.connect(
            database=os.environ.get('DATABASE'),
            user=os.environ.get('DBUSER'),
            password=os.environ.get('PASSWORD'),
            host=os.environ.get('HOST'),
            port=os.environ.get('PORT'),
        )
    except Exception as error:
        print(error)
    else:
        cursor = dbconnect.cursor()
        cursor.execute(query.match_database)
        cursor.execute(query.match_info_database)
        cursor.execute(query.player_database)

        dbconnect.commit()

        for match in matches:
            try:
                cursor.execute(query.matches_insert, [*match.values()])
            except pg.errors.UniqueViolation as e:
                print(e)
                dbconnect.rollback()

        
        for match_info in matches_info:
            try:
                cursor.execute(query.match_info_insert, [*match_info.values()])
            except pg.errors.ForeignKeyViolation as e:
                print(e)
                dbconnect.rollback()


        dbconnect.commit()
        for player in players:
            try:
                cursor.execute(query.player_insert, [*player.values()])
            except pg.errors.ForeignKeyViolation as e:
                print(e)
                dbconnect.rollback()


        dbconnect.commit()




load(transform({'matches': get_matches_mock(), 'matches_info': get_matches_info_mock()}))