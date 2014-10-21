#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import os
import argparse
import logging

"""
daily
"""


argp = argparse.ArgumentParser(description='Attempt on detection of exploding hosts (cache misses)', add_help=False)
argp.add_argument('-h', '--host', default='localhost')
argp.add_argument('-p', '--port', default=5432, type=int)
argp.add_argument('-d', '--dbname', required=True)
argp.add_argument('-U', '--user')
argp.add_argument('-v', '--verbose', action='store_true', default=False)  # default is cronjob mode - no talk
args = argp.parse_args()

if not args.user:
    args.user = os.getenv('PGUSER')
    if not args.user:
        print '--user is required if no PGUSER set'
        exit(1)

logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.WARNING))

conn = psycopg2.connect(host=args.host, port=args.port, database=args.dbname, user=args.user)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

cur.execute("""set statement_timeout to '30min' """)

q_sproc_1h = """
    select * from hosts_evaluate_performance_last_day(null)
    """
cur.execute(q_sproc_1h)
for p in cur.fetchall():
    logging.warning('Host cache miss jump found on HOST: %s, CACHE_MISSES: %s, INCREASE: %s%%', p['host_name'],
                    p['cache_misses'], p['time_percent_increase'])
