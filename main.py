import os

from mwrogue.esports_client import EsportsClient
import mysql.connector
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

    won = 1 if won == 'Yes' else 0

    timestamp = match.get('DateTime UTC')

    if timestamp is None:
        logging.error('timestamp is None')

        return None

    vod = match.get('VOD')

    if vod is not None:
        vod = vod.replace('&amp;', '&')

    return game_id, player, tournament, won, timestamp, vod


def save_match(connection, match):
    cursor = connection.cursor()

    insert_statement = ('INSERT INTO `jhin_picks` (`game_id`, `player`, `tournament`, `won`, `timestamp`, `vod`) '
                        'VALUES (%s, %s, %s, %s, %s, %s) '
                        'ON DUPLICATE KEY UPDATE `game_id` = VALUES(`game_id`), '
                        '`player` = VALUES(`player`), '
                        '`tournament` = VALUES(`tournament`), '
                        '`won` = VALUES(`won`), '
                        '`timestamp` = VALUES(`timestamp`), '
                        '`notified` = `vod` <=> VALUES(`vod`), '
                        '`vod` = VALUES(`vod`)')

    parameters = get_parameters(match)

    try:
        cursor.execute(insert_statement, parameters)

        connection.commit()
    except mysql.connector.Error as e:
        error_string = str(e)

        print("Error:", error_string)


def save_matches(matches):
    host = os.environ['DATABASE_HOST']

    dbname = os.environ['DATABASE_NAME']

    user = os.environ['DATABASE_USERNAME']

    password = os.environ['DATABASE_PASSWORD']

    connection = mysql.connector.connect(user=user, password=password, host=host, database=dbname)

    for match in matches:
        save_match(connection, match)

    connection.close()


def check_for_matches():
    matches = get_matches()

    save_matches(matches)


if __name__ == '__main__':
    check_for_matches()
