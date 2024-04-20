import os

import psycopg
import rollbar
from mwrogue.esports_client import EsportsClient
import hashlib
import logging
from Champion import Champion


def get_matches(champion):
    client = EsportsClient(wiki='lol', max_retries=10)

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

    matches = []

    try:
        matches = client.cargo_client.query(tables=tables, fields=fields, join_on=join_on,
                                            where=f'ScoreboardPlayers.Champion = \'{champion.value}\'',
                                            order_by='ScoreboardPlayers.DateTime_UTC DESC', limit=500)
    except Exception as e:
        error_string = str(e)

        logging.error(error_string)

        rollbar.report_exc_info()

    return matches


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

        rollbar.report_message('game_id is None')

        return None

    player = match.get('Link')

    if player is None:
        logging.error('player is None')

        rollbar.report_message('player is None')

        return None

    tournament = match.get('Tournament')

    if tournament is None:
        logging.error('tournament is None')

        rollbar.report_message('tournament is None')

        return None

    won = match.get('PlayerWin')

    if won is None:
        logging.error('won is None')

        rollbar.report_message('won is None')

        return None

    won = won == 'Yes'

    timestamp = match.get('DateTime UTC')

    if timestamp is None:
        logging.error('timestamp is None')

        rollbar.report_message('timestamp is None')

        return None

    vod = match.get('VOD')

    if vod is not None:
        vod = vod.replace('&amp;', '&')

    return game_id, player, tournament, won, timestamp, vod


def save_match(cursor, champion, match):
    if champion == Champion.JHIN:
        table_name = 'jhin_picks'
    elif champion == Champion.LUCIAN:
        table_name = 'lucian_picks'
    else:
        message = f'Unknown champion: {champion.value}'

        rollbar.report_message(message)

        return

    insert_statement = (f'INSERT INTO "{table_name}" ("game_id", "player", "tournament", "won", "timestamp", "vod") '
                        'VALUES (%s, %s, %s, %s, %s, %s) '
                        'ON CONFLICT ("game_id") DO UPDATE '
                        'SET "game_id" = EXCLUDED."game_id", '
                        '"player" = EXCLUDED."player", '
                        '"tournament" = EXCLUDED."tournament", '
                        '"won" = EXCLUDED."won", '
                        '"timestamp" = EXCLUDED."timestamp", '
                        f'"notified" = "{table_name}"."vod" IS NOT DISTINCT FROM EXCLUDED."vod", '
                        '"vod" = EXCLUDED."vod"')

    parameters = get_parameters(match)

    if parameters is None:
        return

    try:
        cursor.execute(insert_statement, parameters)
    except psycopg.Error as e:
        error_string = str(e)

        logging.error(error_string)

        rollbar.report_exc_info()


def save_matches(champion_to_matches):
    host = os.environ.get('DATABASE_HOST')

    if host is None:
        logging.error('DATABASE_HOST is not set')

        rollbar.report_message('DATABASE_HOST is not set')

        exit(1)

    dbname = os.environ.get('DATABASE_NAME')

    if dbname is None:
        logging.error('DATABASE_NAME is not set')

        rollbar.report_message('DATABASE_NAME is not set')

        exit(1)

    user = os.environ.get('DATABASE_USERNAME')

    if user is None:
        logging.error('DATABASE_USERNAME is not set')

        rollbar.report_message('DATABASE_USERNAME is not set')

        exit(1)

    password = os.environ.get('DATABASE_PASSWORD')

    if password is None:
        logging.error('DATABASE_PASSWORD is not set')

        rollbar.report_message('DATABASE_PASSWORD is not set')

        exit(1)

    with psycopg.connect(host=host, dbname=dbname, user=user, password=password) as connection:
        with connection.cursor() as cursor:
            for champion, matches in champion_to_matches.items():
                for match in matches:
                    save_match(cursor, champion, match)

            connection.commit()


def check_for_matches():
    jhin_matches = get_matches(champion=Champion.JHIN)

    lucian_matches = get_matches(champion=Champion.LUCIAN)

    champion_to_matches = {
        Champion.JHIN: jhin_matches,
        Champion.LUCIAN: lucian_matches
    }

    save_matches(champion_to_matches)


if __name__ == '__main__':
    rollbar_access_token = os.environ.get('ROLLBAR_ACCESS_TOKEN')

    if rollbar_access_token is None:
        logging.error('ROLLBAR_ACCESS_TOKEN is not set')

        exit(1)

    rollbar_environment = os.environ.get('ROLLBAR_ENVIRONMENT')

    if rollbar_environment is None:
        logging.error('ROLLBAR_ENVIRONMENT is not set')

        exit(1)

    rollbar_code_version = os.environ.get('ROLLBAR_CODE_VERSION')

    if rollbar_code_version is None:
        logging.error('ROLLBAR_CODE_VERSION is not set')

        exit(1)

    rollbar.init(access_token=rollbar_access_token, environment=rollbar_environment, code_version=rollbar_code_version)

    check_for_matches()
