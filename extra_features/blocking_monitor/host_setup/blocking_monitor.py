#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extensions
import argparse
import logging
import time

logging.basicConfig(format='%(asctime)s %(levelname)s - %(message)s', level=logging.ERROR)
logger = logging.getLogger(__name__)

MAX_TIME = 60.0  # aiming for 1 min
time_used = 0.0


def parse_arguments():
    parser = argparse.ArgumentParser(description='Script for gathering blocking locks info into z_blocking schema')
    parser.add_argument('-H', '--host', help='hostname of database', default='localhost')
    parser.add_argument('-p', '--port', help='port', default='5432')
    parser.add_argument('-d', '--db', help='db', required=True)
    parser.add_argument('-v', '--verbose', help='verbose', action='store_true', default=False)

    args = parser.parse_args()
    return args


def get_pg_version(cur):
    sql = """select setting from pg_settings where name = 'server_version_num' """
    cur.execute(sql)
    return int(cur.fetchone()[0])


def is_blocking_schema_there_and_pg_version_ok(cur):
    if get_pg_version(cur) < 90200:
        return False

    sql_schema_check = \
        """select 1 from pg_proc where proname = 'blocking_monitor' and pronamespace = (select oid from pg_namespace where nspname = 'z_blocking')"""
    cur.execute(sql_schema_check)
    return cur.rowcount == 1


def main():
    start_time = time.time()
    args = parse_arguments()

    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)

    conn = psycopg2.connect(host=args.host, port=args.port, dbname=args.db)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    logger.info('conn OK...')

    if not is_blocking_schema_there_and_pg_version_ok(cur):
        logging.error('z_blocking.blocking_monitor() missing or server is running Postgres less than 9.2. cannot continue'
                      )
        exit(1)

    logger.info('starting looping...')
    sql_monitor = """select z_blocking.blocking_monitor()"""

    i = 1
    time_used = time.time() - start_time
    while time_used < MAX_TIME - 1:
        logger.info('doing loop %s', i)
        i += 1
        t1 = time.time()
        cur.execute(sql_monitor)
        t2 = time.time()
        time_to_sleep = (1.0 - (t2 - t1) if t2 - t1 < 1.0 else 0.9)  # aiming for ~1s loop time but sleeping at least 0.9s
        time.sleep(time_to_sleep)
        time_used = time.time() - start_time

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
