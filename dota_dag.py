import logging
import os
import json
import requests
import psycopg2 as pg
import time
from psycopg2.extras import Json
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import constants
import fields as fields
import query as  query


def get_matches(mmr=None):
    logging.info('Start get_matches handler for mmr %s', mmr)
    params = {'mmr_ascending': mmr}
    request = requests.get(
        constants.API_ROOT + '/api/publicMatches',
        params=params,
    )
    if request.status_code == 200:
        logging.info('Successfully collected matches')
        return json.loads(request.content)
    logging.error('Error getting matches %s', request.content)

def get_match(match_id):
    logging.info('Start get_match handler for match id %s', match_id)

    request = requests.get(
        constants.API_ROOT + '/api/matches/{}'.format(match_id)
    )
    if request.status_code == 200:
        logging.info('Math successfully collected')
        return json.loads(request.content)
    logging.error('Error getting match %s', request.content)

default_args = {
    'owner': 'airflow',
}
@dag(default_args=default_args, schedule_interval='@daily', start_date=days_ago(2))
def dota_matches():
    @task(multiple_outputs=True)
    def extract():
        logging.info('Start extract task')
        matches = get_matches(6000)
        matches_info = []
        th = 1
        for match in matches:
            if th % constants.API_RATE == 0:
                time.sleep(60)
            th+=1
            matches_info.append(
                get_match(match.get('match_id'))
            )

        logging.info('Extract task finish')
        logging.info('Passing %s matches and %s matches_info', len(matches), len(matches_info))
        return {'matches': matches, 'matches_info': matches_info} 
    @task(multiple_outputs=True)
    def transform(data: dict):
        logging.info('Start Transform task')
        matches = data.get('matches')
        matches_info = data.get('matches_info')

        _matches_info = []
        _matches = []
        players = []
        logging.info('Processing matches')
        for match in matches:
            if match:
                _matches.append(
                    {fv: match[fk] if fk in match.keys() else None for fk, fv in fields.matches_fields.items()}
                )
        logging.info('Processing matches info')
        for match in matches_info:
            match['start_time'] = datetime.fromtimestamp(match['start_time']).strftime("%Y/%m/%d, %H:%M:%S")


            if match:
                _players = []
                match_id = match.get('match_id')
                _matches_info.append({
                    fv: match[fk] if fk in match.keys() else None for fk, fv in fields.match_info_fields.items()
                })

                for player in match.get('players'):
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
        logging.info('Passing %s matches and %s matches_info and players %s', len(matches), len(matches_info), len(players))
        return {'matches': _matches, 'matches_info': _matches_info, 'players': players}


    @task()
    def load(data: dict):
        logging.info('Start Load task')
        matches = data.get('matches')
        matches_info = data.get('matches_info')
        players = data.get('players')
        logging.info('Creating databases')
        try:
            dbconnect = pg.connect(
                database=constants.DATABASE,
                user=constants.DBUSER,
                password=constants.PASSWORD,
                host=constants.HOST,
                port=constants.PORT,
            )
        except Exception as error:
            print(error)
            raise Exception("Cannot connect to the database")
        else:
            cursor = dbconnect.cursor()
            cursor.execute(query.match_database)
            cursor.execute(query.match_info_database)
            cursor.execute(query.player_database)

            dbconnect.commit()
            logging.info('Inserting matches')
            for match in matches:
                try:
                    cursor.execute(query.matches_insert, [*match.values()])
                except pg.errors.UniqueViolation as e:
                    print(e)
                    dbconnect.rollback()
                else:
                    dbconnect.commit()

            logging.info('Inserting matches info')
            for match_info in matches_info:
                try:
                    match_info['draft_timings'] = [Json(match_info['draft_timings'])]
                    cursor.execute(query.match_info_insert, [*match_info.values()])
                except pg.errors.ForeignKeyViolation as e:
                    print(e)
                    dbconnect.rollback()
                else:
                    dbconnect.commit()

            logging.info('Inserting players')
            for player in players:
                try:
                    player['permanent_buffs'] = [Json(player['permanent_buffs'])]
                    cursor.execute(query.player_insert, [*player.values()])
                except pg.errors.ForeignKeyViolation as e:
                    print(e)
                    dbconnect.rollback()
                else:
                    dbconnect.commit()

        logging.info('ETL successfully executed')




    matches = extract()
    matches = transform(matches)
    load(matches)
dota_etl_dag = dota_matches()