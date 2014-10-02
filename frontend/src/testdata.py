#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras
import json
import os.path
import random

from argparse import ArgumentParser


def create_hosts(cur, host_id, host_name, host_db):
    sql = """
        INSERT INTO hosts (host_id,host_name,host_db,host_enabled,
                           host_ui_shortname,host_ui_longname,
                           host_settings)
                     VALUES(%s,%s,%s,true,
                            %s,%s,
                          '{"loadGatherInterval": 5,
                            "tableIoGatherInterval": 5,
                            "sprocGatherInterval": 5,
                            "indexStatsGatherInterval": 5,
                            "schemaStatsGatherInterval": 5,
                            "tableStatsGatherInterval": 5,
                            "blockingStatsGatherInterval": 0,
                            "statStatementsGatherInterval": 0,
                            "statDatabaseGatherInterval": 5
                            }')
        """
    cur.execute(sql,
                (
                    host_id,
                    host_name,
                    host_db,
                    host_name,
                    host_name.capitalize().replace('-', ' '),
                ))


def clear_test_data(cur, host_id):
    print 'Clearing test data...'

    cur.execute('DELETE FROM sproc_performance_data WHERE sp_sproc_id IN ( SELECT sproc_id FROM sprocs WHERE sproc_host_id = %s )'
                , (host_id, ))
    cur.execute('DELETE FROM sprocs WHERE sproc_host_id = %s', (host_id, ))
    cur.execute('DELETE FROM table_size_data WHERE tsd_table_id IN ( SELECT t_id FROM tables WHERE t_host_id = %s )',
                (host_id, ))
    cur.execute('DELETE FROM table_io_data WHERE tio_table_id IN ( SELECT t_id FROM tables WHERE t_host_id = %s ) ',
                (host_id, ))
    cur.execute('DELETE FROM tables WHERE t_host_id = %s ', (host_id, ))
    cur.execute('DELETE FROM host_load WHERE load_host_id = %s', (host_id, ))
    cur.execute('DELETE FROM indexes WHERE i_host_id = %s', (host_id, ))
    cur.execute('DELETE FROM index_usage_data WHERE iud_host_id = %s', (host_id, ))
    cur.execute('DELETE FROM schema_usage_data WHERE sud_host_id = %s', (host_id, ))
    cur.execute('DELETE FROM stat_database_data WHERE sdd_host_id = %s', (host_id, ))
    cur.execute('DELETE FROM hosts WHERE host_id = %s', (host_id, ))


def create_n_sprocs(cur, host_id, n):
    print 'Creating stored procedures...'

    i = 0
    while i < n:
        cur.execute('INSERT INTO sprocs ( sproc_host_id , sproc_schema , sproc_name ) VALUES ( %s , %s, %s ) ',
                    (host_id, 'apischema', 'stored_proc_' + str(i) + '(a int, b text)'))
        i += 1


def create_n_tables(cur, host_id, n):
    print 'Creating tables...'

    i = 0
    while i < n:
        cur.execute('INSERT INTO tables ( t_host_id , t_name, t_schema ) VALUES ( %s , %s, %s ) ', (host_id, 'table_'
                    + str(i), 'dataschema'))
        i += 1


def create_sproc_data(cur, host_id, days, interval):
    print 'Creating stored procedure data points...'

    cur.execute("SELECT 'now'::timestamp - ('1 day'::interval * %s)", (days, ))
    n = cur.fetchone()

    rows = days * 24 * 60 / interval
    print '\tInserting ' + str(rows) + ' data points for each procedure'

    cur.execute('SELECT sproc_id FROM sprocs WHERE sproc_host_id = %s', (host_id, ))
    sprocs = cur.fetchall()
    j = 1

    for s in sprocs:

        i = 0
        print '\t\tInserting data for sproc_id: ' + str(s[0])

        c = 0
        st = 0
        tt = 0

        while i < rows:

            cur.execute("""INSERT INTO sproc_performance_data(sp_timestamp, sp_sproc_id, sp_calls, sp_self_time, sp_total_time, sp_host_id)
                                                    VALUES (%s + (%s * '%s minute'::interval),%s,%s,%s,%s,%s) """
                        , (
                n,
                i,
                interval,
                s[0],
                c,
                st * 2000,
                tt * 2000,
                host_id,
            ))

            c += i % (50 / interval) * 3 + j
            st += i % (50 / interval) * 6 + j * 4
            tt += i % (50 / interval) * 9 + j * 5

            i += 1

        j += 1


def create_load_data(cur, host_id, days, interval):
    print 'Creating load data points...'
    cur.execute("SELECT 'now'::timestamp - ('1 day'::interval * %s)", (days, ))
    n = cur.fetchone()

    rows = days * 24 * 60 / interval
    print '\tInserting ' + str(rows) + ' data points for Load'

    i = 0
    xlog = 0
    while i < rows:
        l1 = random.randint(300, 2000)
        l5 = random.randint(200, 1500)
        l15 = random.randint(100, 1000)
        xlog += (500 + random.randint(0, 200)) + (random.randint(300, 800) if random.random() < 0.2 else 0)
        cur.execute("""INSERT INTO host_load(load_host_id, load_timestamp, load_1min_value, load_5min_value, load_15min_value, xlog_location, xlog_location_mb)
                                     VALUES (%s,%s + (%s * '%s minute'::interval),%s,%s,%s,%s,%s) """
                    , (
            host_id,
            n,
            i,
            interval,
            l1,
            l5,
            l15,
            'X/X',
            xlog,
        ))
        i += 1


def create_general_db_stats(cur, host_id, days, interval):
    print 'Creating general db stats...'
    cur.execute("SELECT 'now'::timestamp - ('1 day'::interval * %s)", (days, ))
    n = cur.fetchone()

    rows = days * 24 * 60 / interval

    i = 0
    xact_rollback = 0
    temp_bytes = 0L
    deadlocks = 0
    while i < rows:
        xact_rollback += random.randint(0,4) if random.random() < 0.1 else 1
        deadlocks += random.randint(0,2) if random.random() < 0.1 else 0
        temp_bytes += 10**8 + (random.randint(0, 10**7) if random.random() < 0.05 else 0)
        cur.execute("""INSERT INTO monitor_data.stat_database_data(
                        sdd_host_id, sdd_timestamp,
                        sdd_numbackends, sdd_xact_rollback, --sdd_xact_commit, sdd_blks_read, sdd_blks_hit,
                        sdd_temp_bytes, sdd_deadlocks --sdd_temp_files, sdd_blk_read_time, sdd_blk_write_time
                    )
                    VALUES (%s,%s + (%s * '%s minute'::interval),
                            %s,%s,
                            %s, %s)
                    """
                    , (
            host_id,n,i,interval,
            100 if random.random() < 0.8 else random.randint(90, 120), xact_rollback,
            temp_bytes, deadlocks
        ))
        i += 1


def create_table_data(cur, host_id, days, interval):
    print 'Creating table data points...'

    cur.execute("SELECT 'now'::timestamp - ('1 day'::interval * %s)", (days, ))
    n = cur.fetchone()
    cur.execute('select t_id from tables where t_host_id = %s', (host_id, ))
    tables = [x[0] for x in cur.fetchall()]

    rows = days * 24 * 60 / interval
    i = 0
    tbl_size = 10 ** 6 * 5
    ind_size = 10 ** 6
    scans = 100
    iscans = 1000
    tupi = 300
    tupu = 200
    tupd = 100
    tupuh = 100
    hread = hhit = iread = ihit = 1000
    while i < rows:
        tbl_size += random.randint(-100000, 200000)
        ind_size += random.randint(-50000, 100000)
        scans = (scans + 2 if random.random() < 0.2 else scans)
        iscans = (iscans + 10 if random.random() < 0.2 else iscans)
        if random.random() < 0.1:
            tupi += 1000
            tupu += 900
            hread += 100
            iread += 100
        if random.random() < 0.1:
            tupd += random.randint(500, 800)
            tupuh += random.randint(400, 700)
            hhit += 100
            ihit += 100
        for table in tables:
            cur.execute("""INSERT INTO table_size_data(tsd_table_id, tsd_timestamp, tsd_table_size, tsd_index_size, tsd_seq_scans, tsd_index_scans,
                                                        tsd_tup_ins, tsd_tup_upd, tsd_tup_del, tsd_tup_hot_upd, tsd_host_id)
                                         VALUES (%s,%s + (%s * '%s minute'::interval),%s,%s,%s,%s,%s,%s,%s,%s,%s) """,
                        (
                table,
                n,
                i,
                interval,
                tbl_size,
                ind_size,
                scans,
                iscans,
                tupi,
                tupu,
                tupd,
                tupuh,
                host_id,
            ))
            cur.execute("""INSERT INTO table_io_data(tio_table_id, tio_timestamp, tio_heap_read, tio_heap_hit, tio_idx_read, tio_idx_hit, tio_host_id)
                                         VALUES (%s,%s + (%s * '%s minute'::interval), %s,%s,%s,%s,%s) """
                        , (
                table,
                n,
                i,
                interval,
                hread,
                hhit,
                iread,
                ihit,
                host_id,
            ))
        i += 1


def generate_test_data(connection_url, hostcount, tables, sprocs, days, interval):
    conn = psycopg2.connect(connection_url)
    try:
        cur = conn.cursor()
        cur.execute("SET work_mem TO '64MB';")
        cur.execute('SET search_path TO monitor_data, public;')
        conn.commit()

        for i in xrange(0, hostcount):
            host_id = 1000 + i
            print 'Doing host with host_id =', host_id
            clear_test_data(cur, host_id)
            create_hosts(cur, host_id, 'test-cluster-{}'.format(i), 'test{}_db'.format(i))
            create_n_sprocs(cur, host_id, sprocs)
            create_n_tables(cur, host_id, tables)
            create_table_data(cur, host_id,days, interval)
            create_sproc_data(cur, host_id,days, interval)
            create_load_data(cur, host_id, days, interval)
            create_general_db_stats(cur, host_id, days, interval)
            conn.commit()
    finally:
        if conn != None:
            conn.close()


# ".test" is added just to make sure it's not executed by default on a production db
DEFAULT_CONF_FILE = '~/.pgobserver.conf.test'


def main():
    parser = ArgumentParser(description='PGObserver testdata generator')
    parser.add_argument('-c', '--config', help='Path to config file. (default: %s)' % DEFAULT_CONF_FILE, dest='config',
                        default=DEFAULT_CONF_FILE)
    parser.add_argument('-gh', '--generate-x-hosts', help='Number of hosts', dest='gh', default=2, type=int)
    parser.add_argument('-gts', '--generate-x-tables', help='Number of tables', dest='gt', default=5)
    parser.add_argument('-gps', '--generate-x-procs', help='Number of stored procedures', dest='gp', default=10)
    parser.add_argument('-gds', '--generate-x-days', help='Number of days', dest='gd', default=15)
    parser.add_argument('-giv', '--generate-interval', help='Interval between data in minutes', dest='gi', default=5)

    args = parser.parse_args()

    args.config = os.path.expanduser(DEFAULT_CONF_FILE)

    if not os.path.exists(args.config):
        print 'Configuration file missing:', DEFAULT_CONF_FILE
        parser.print_help()
        return

    with open(args.config, 'rb') as fd:
        settings = json.load(fd)

    connection_url = ' '.join((
        'host=' + settings['database']['host'],
        'port=' + str(settings['database']['port']),
        'user=' + settings['database']['backend_user'],
        'password=' + settings['database']['backend_password'],
        'dbname=' + settings['database']['name'],
    ))

    print 'PGObserver testdata generator:'
    print '=============================='
    print ''
    print 'Setting connection string to ... ' + connection_url
    print ''
    print 'Creating ' + str(args.gh) + ' hosts'
    print 'with ' + str(args.gt) + ' tables'
    print 'with ' + str(args.gp) + ' stored procedures'
    print 'with ' + str(args.gd) + ' days of data'
    print 'with data points every ' + str(args.gi) + ' minutes'
    print ''

    generate_test_data(connection_url, args.gh, args.gt, args.gp, args.gd, args.gi)


if __name__ == '__main__':
    main()
