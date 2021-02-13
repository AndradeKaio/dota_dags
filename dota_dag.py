import os
import json
import requests
import psycopg2 as pg
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import fields as fields
import query as  query


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

default_args = {
    'owner': 'airflow',
}
@dag(default_args=default_args, schedule_interval='@daily', start_date=days_ago(2))
def dota_matches():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """
    @task(multiple_outputs=True)
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        matches = get_matches(6000)
        matches_info = []
        for match in matches:
            matches_info.append(
                get_match(match.get('match_id'))
            )

        return {'matches': matches, 'matches_info': matches_info} 
    @task(multiple_outputs=True)
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


    @task()
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



    matches = extract()
    matches = transform(matches)
    load(matches)
dota_etl_dag = dota_matches()