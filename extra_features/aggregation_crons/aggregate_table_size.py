import psycopg2
import psycopg2.extras
import argparse
import os


argp = argparse.ArgumentParser(description='Aggregate table size for faster display', add_help=False)

argp.add_argument('-h', '--host', dest='host', required=True)
argp.add_argument('-p', '--port', dest='port', default=5432)
argp.add_argument('-U', '--user', dest='user', default=os.getenv('PGUSER'))
argp.add_argument('-d', '--database', dest='database', required=True)
argp.add_argument('-i', '--interval', dest='interval', required=True, default='8 days', help='how far back to look. needs to be a valid SQL interval')

args = argp.parse_args()

conn = psycopg2.connect(host=args.host, port=args.port, dbname=args.database, user=args.user)
conn.autocommit = True
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

sql_hosts = """
    select host_id
    from monitor_data.hosts
    where host_enabled
    order by host_id
    """

cur.execute(sql_hosts)
hosts = cur.fetchall()

sql_do_aggregate = """
    select * from monitor_data.aggregate_table_size_data(%s,%s)
    """
for host in hosts:
    cur.execute(sql_do_aggregate, (args.interval, [host['host_id']]))

