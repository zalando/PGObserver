#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import os
import argparse
import logging


"""
hourly
"""


argp = argparse.ArgumentParser(description='Attempt on auto-detection of gradually exploding sprocs', add_help=False)
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

PGO_FRONTEND_URL = 'http://'
logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.WARNING))

conn = psycopg2.connect(host=args.host, port=args.port, database=args.dbname, user=args.user)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# TODO abnormality_findings tbl, start logging found issues

q_hosts = """select host_id, host_name, host_db, host_port, host_ui_shortname from hosts"""
cur.execute(q_hosts)
hosts = cur.fetchall()
h_map = {}
for h in hosts:
    h_map[h['host_name']] = {
        'host_ui_shortname': h['host_ui_shortname'],
        'host_db': h['host_db'],
        'host_port': h['host_port'],
        'host_id': h['host_id'],
    }

q_sproc_1h = """
    select * from sprocs_evaluate_performance_last_hour (null, null, 'true','false')
    """
cur.execute(q_sproc_1h)

for p in cur.fetchall():
    host_name = p['host_name'][0]
    logging.warning('----')
    logging.warning('Sproc jump found on HOST: %s, SPROC: %s, CALLS: %s, AVG_TIME: %s, INCREASE: %s %%', host_name,
                    p['sproc_name'], p['calls'], p['avg_time'], p['time_percent_increase'])
    logging.warning("""psql_pgobserver_LIVE -c "select * from sproc_get_prev_values(%s, '%s') """,
                    h_map[host_name]['host_id'], p['sproc_name'])

q_total_sproc_runtime_1h = \
    """
    select * from sprocs_evaluate_total_performance_last_hour (null, null, 'true','false')
    """
cur.execute(q_total_sproc_runtime_1h)
# OUT host_name text[], OUT calls bigint, OUT total_time bigint, OUT avg_time bigint, OUT time_percent_increase integer

for p in cur.fetchall():
    host_name = p['host_name'][0]
    logging.warning('----')
    logging.warning('Total sproc runtime jump found for last hour on HOST: %s, CALLS: %s, TOTAL_TIME: %s, AVG_TIME: %s, AVG TIME INCREASE: %s %%'
                    , host_name, p['calls'], p['total_time'], p['avg_time'], p['time_percent_increase'])
    logging.warning("""%s/%s""", PGO_FRONTEND_URL, h_map[host_name]['host_id'])
