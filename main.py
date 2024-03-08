import os

import psycopg
import rollbar
from mwrogue.esports_client import EsportsClient
import hashlib
import logging


def get_matches():
    client = EsportsClient('lol')

    tables = [
        'ScoreboardPlayers',
        'ScoreboardGames'
    ]

    fields = [
        'ScoreboardPlayers.GameId',
        'ScoreboardPlayers.Link',
        'ScoreboardGames.Tournament',
        'ScoreboardPlayers.DateTime_UTC',
        'ScoreboardPlayers.PlayerWin',
        'ScoreboardGames.VOD'
    ]

    join_on = [
        'ScoreboardPlayers.GameId = ScoreboardGames.GameId'
    ]

    return client.cargo_client.query(tables=tables, fields=fields, join_on=join_on,
                                     where="ScoreboardPlayers.Champion = 'Jhin'",
                                     order_by='ScoreboardPlayers.DateTime_UTC DESC', limit=500)


def get_game_id(match):
    game_id = match.get('GameId')

    if game_id is None:
        return None

    game_id_bytes = game_id.encode()

    return (hashlib.sha256(game_id_bytes)
            .hexdigest())


def get_parameters(match):
    game_id = get_game_id(match)

    if game_id is None:
        logging.error('game_id is None')

        return None

    player = match.get('Link')

    if player is None:
        logging.error('player is None')

        return None

    tournament = match.get('Tournament')

    if tournament is None:
        logging.error('tournament is None')

        return None

    won = match.get('PlayerWin')

    if won is None:
        logging.error('won is None')

        return None

    won = won == 'Yes'

    timestamp = match.get('DateTime UTC')

    if timestamp is None:
        logging.error('timestamp is None')

        return None

    vod = match.get('VOD')

    if vod is not None:
        vod = vod.replace('&amp;', '&')

    return game_id, player, tournament, won, timestamp, vod


def save_match(cursor, match):
    insert_statement = ('INSERT INTO "jhin_picks" ("game_id", "player", "tournament", "won", "timestamp", "vod") '
                        'VALUES (%s, %s, %s, %s, %s, %s) '
                        'ON CONFLICT ("game_id") DO UPDATE '
                        'SET "game_id" = EXCLUDED."game_id", '
                        '"player" = EXCLUDED."player", '
                        '"tournament" = EXCLUDED."tournament", '
                        '"won" = EXCLUDED."won", '
                        '"timestamp" = EXCLUDED."timestamp", '
                        '"notified" = "jhin_picks"."vod" IS NOT DISTINCT FROM EXCLUDED."vod", '
                        '"vod" = EXCLUDED."vod"')

    parameters = get_parameters(match)

    try:
        cursor.execute(insert_statement, parameters)
    except psycopg.Error as e:
        error_string = str(e)

        print('Error: ', error_string)

        rollbar.report_exc_info()


def save_matches(matches):
    host = os.environ['DATABASE_HOST']

    dbname = os.environ['DATABASE_NAME']

    user = os.environ['DATABASE_USERNAME']

    password = os.environ['DATABASE_PASSWORD']

    with psycopg.connect(host=host, dbname=dbname, user=user, password=password) as connection:
        with connection.cursor() as cursor:
            for match in matches:
                save_match(cursor, match)

            connection.commit()


def check_for_matches():
    matches = get_matches()

    save_matches(matches)


if __name__ == '__main__':
    rollbar_access_token = os.environ.get('ROLLBAR_ACCESS_TOKEN')

    if rollbar_access_token is None:
        print('ROLLBAR_ACCESS_TOKEN is not set')

        exit(1)

    rollbar_environment = os.environ.get('ROLLBAR_ENVIRONMENT')

    if rollbar_environment is None:
        print('ROLLBAR_ENVIRONMENT is not set')

        exit(1)

    rollbar_code_version = os.environ.get('ROLLBAR_CODE_VERSION')

    if rollbar_code_version is None:
        print('ROLLBAR_CODE_VERSION is not set')

        exit(1)

    rollbar.init(access_token=rollbar_access_token, environment=rollbar_environment, code_version=rollbar_code_version)

    check_for_matches()
