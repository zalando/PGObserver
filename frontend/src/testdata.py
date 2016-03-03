#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras
import time
import yaml
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
    cur.execute('DELETE FROM stat_statements_data WHERE ssd_host_id = %s', (host_id, ))
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


def create_sproc_data(cur, host_id, nr_of_sprocs, days, interval):
    create_n_sprocs(cur, host_id, nr_of_sprocs)

    print 'Creating stored procedure data points...'

    cur.execute('SELECT sproc_id FROM sprocs WHERE sproc_host_id = %s', (host_id, ))
    sproc_ids = [x[0] for x in cur.fetchall()]

    for s in sproc_ids:
        print '\t\tInserting data for sproc_id:', s

        sql = """INSERT INTO sproc_performance_data(
                                sp_timestamp, sp_host_id, sp_sproc_id,
                                sp_calls, sp_self_time, sp_total_time)
                    SELECT
                        tz, %(host_id)s, %(sproc_id)s,
                        epoch + ((epoch - epoch_lag)* (sin(i/10.0)))::int,
                        epoch*1000 + ((epoch - epoch_lag)* (sin(i/10.0)))::int,
                        epoch*1000 + ((epoch - epoch_lag)* (sin(i/10.0)))::int
                    FROM (
                        select
                          tz,
                          rank() over(order by tz) as i,
                          lag(extract(epoch from tz)) over(order by tz) as epoch_lag,
                          extract(epoch from tz) as epoch
                        from
                          generate_series(current_date - %(days)s, now(), %(interval)s::interval) tz
                        ) a
                  """
        cur.execute(sql, {'host_id': host_id, 'days': days, 'sproc_id': s, 'interval': interval})


def create_stat_statement_data(cur, host_id, nr_of_statements, days, interval):
    create_n_sprocs(cur, host_id, nr_of_statements)

    print 'Creating stat_statement data points...'
    stmts = ["select somequery{};".format(x) for x in range(nr_of_statements)]
    stmt_ids = [("select somequery{};".format(x)).__hash__() for x in range(nr_of_statements)]

    for stmt, stmt_id in zip(stmts, stmt_ids):
        print '\t\tInserting data for statement:', stmt

        sql = """INSERT INTO monitor_data.stat_statements_data(
                                ssd_timestamp, ssd_host_id,
                                ssd_query, ssd_query_id,
                                ssd_calls, ssd_total_time,
                                ssd_blks_read, ssd_blks_written,
                                ssd_temp_blks_read, ssd_temp_blks_written)
                    SELECT
                        tz, %(host_id)s,
                        %(stmt)s, %(stmt_id)s,
                        i*100 + i*10*abs((sin(i/10.0)))::int, epoch,
                        epoch, epoch,
                        epoch, epoch
                    FROM (
                        select
                          tz,
                          rank() over(order by tz) as i,
                          lag(extract(epoch from tz)) over(order by tz) as epoch_lag,
                          extract(epoch from tz) as epoch
                        from
                          generate_series(current_date - %(days)s, now(), %(interval)s::interval) tz
                        ) a
                  """
        cur.execute(sql, {'host_id': host_id, 'days': days, 'stmt': stmt, 'stmt_id': stmt_id, 'interval': interval})


def create_load_data(cur, host_id, days, interval):
    print 'Inserting data points for Load'

    sql = """
        INSERT INTO monitor_data.host_load(
            load_host_id, load_timestamp,
            load_1min_value, load_5min_value, load_15min_value,
            xlog_location_mb)
          SELECT
            %(host_id)s, tz,
            10000 * abs(sin(i / 100.0)),
            10000 * abs(sin(i / 100.0)),
            10000 * abs(sin(i / 100.0)),
            epoch*1000 + abs(sin(i / 100.0))* 10^7
          FROM (
            select
              tz,
              extract(epoch from tz) as epoch,
              rank() over(order by tz) as i
            from
              generate_series(current_date - %(days)s, now(), %(interval)s::interval) tz
            ) a;
    """
    cur.execute(sql, {'host_id': host_id, 'days': days, 'interval': interval})


def create_general_db_stats(cur, host_id, days, interval):
    print 'Creating general db stats...'
    sql = """
            INSERT INTO monitor_data.stat_database_data(
                        sdd_host_id, sdd_timestamp,
                        sdd_numbackends, sdd_xact_rollback, --sdd_xact_commit, sdd_blks_read, sdd_blks_hit,
                        sdd_temp_bytes, sdd_deadlocks) --sdd_temp_files, sdd_blk_read_time, sdd_blk_write_time
                SELECT
                  %(host_id)s, tz,
                  100 + 20*sin(epoch/30000.0), 20 + 5*sin(epoch/30000.0),
                  abs(1000000*sin(epoch/30000.0)), 2 + 2*sin(epoch/30000.0)
                FROM (
                    select
                      tz,
                      extract(epoch from tz) as epoch
                    from
                      generate_series(current_date - %(days)s, now(), %(interval)s::interval) tz
                    ) a;
                    """
    cur.execute(sql, {'host_id': host_id, 'days': days, 'interval': interval})


def create_table_data(cur, host_id, nr_of_tables, days, interval):
    create_n_tables(cur, host_id, nr_of_tables)

    print 'Creating table data points...'

    cur.execute('select t_id from tables where t_host_id = %s', (host_id, ))
    tables = [x[0] for x in cur.fetchall()]

    for table in tables:
        sql = """INSERT INTO table_size_data(
                            tsd_host_id, tsd_table_id, tsd_timestamp,
                            tsd_table_size, tsd_index_size,
                            tsd_seq_scans, tsd_index_scans,
                            tsd_tup_ins, tsd_tup_upd, tsd_tup_del, tsd_tup_hot_upd)
                    SELECT
                      %(host_id)s, %(table_id)s, tz,
                      epoch*10, epoch*2,
                      epoch*2, epoch*4,
                      epoch*4, epoch*2, epoch, epoch / 2
                    FROM (
                        select
                          tz,
                          extract(epoch from tz) as epoch
                        from
                          generate_series(current_date - %(days)s, now(), %(interval)s::interval) tz
                        ) a;
                 """
        cur.execute(sql, {'host_id': host_id, 'table_id': table, 'days': days, 'interval': interval})


        sql = """INSERT INTO table_io_data(
                            tio_host_id, tio_table_id, tio_timestamp,
                            tio_heap_read, tio_heap_hit, tio_idx_read, tio_idx_hit)
                    SELECT
                        %(host_id)s, %(table_id)s, tz,
                        (epoch/100)*2, (epoch/100)*10, (epoch/100)*2, (epoch/100)*10
                    FROM (
                            select
                              tz,
                              extract(epoch from tz) as epoch
                            from
                              generate_series(current_date - %(days)s, now(), %(interval)s::interval) tz
                        ) a;
                  """
        cur.execute(sql, {'host_id': host_id, 'table_id': table, 'days': days, 'interval': interval})


def generate_test_data(connection_url, hostcount, tables, sprocs, days, interval):
    interval = str(interval) + ' minutes'
    conn = psycopg2.connect(connection_url)
    try:
        cur = conn.cursor()
        cur.execute("SET work_mem TO '64MB';")
        cur.execute('SET search_path TO monitor_data, public;')

        for i in xrange(0, hostcount):
            host_id = 1000 + i
            print 'Doing host "test-cluster-{}" with host_id = {}'.format(i, host_id)
            clear_test_data(cur, host_id)
            create_hosts(cur, host_id, 'test-cluster-{}'.format(i), 'test{}_db'.format(i))
            create_load_data(cur, host_id, days, interval)
            create_table_data(cur, host_id, tables, days, interval)
            create_sproc_data(cur, host_id, sprocs, days, interval)
            create_stat_statement_data(cur, host_id, sprocs, days, interval)
            create_general_db_stats(cur, host_id, days, interval)
    finally:
        if conn != None:
            conn.commit()
            conn.close()


def main():
    parser = ArgumentParser(description='PGObserver testdata generator')
    parser.add_argument('-c', '--config', help='Path to config file. (see gatherer/pgobserver_gatherer.example.yaml for a template)', dest='config')
    parser.add_argument('-gh', '--generate-x-hosts', help='Number of hosts', dest='gh', default=1, type=int)
    parser.add_argument('-gts', '--generate-x-tables', help='Number of tables', dest='gt', default=5)
    parser.add_argument('-gps', '--generate-x-procs', help='Number of stored procedures', dest='gp', default=5)
    parser.add_argument('-gds', '--generate-x-days', help='Number of days', dest='gd', default=10)
    parser.add_argument('-giv', '--generate-interval', help='Interval between data in minutes', dest='gi', default=5)

    args = parser.parse_args()

    args.config = os.path.expanduser(args.config)

    if not os.path.exists(args.config):
        print 'Configuration file missing:', args.config
        parser.print_help()
        return

    print 'Using configuration file:', args.config

    settings = None
    with open(args.config, 'rb') as fd:
        settings = yaml.load(fd)

    connection_url = ' '.join((
        'host=' + settings['database']['host'],
        'port=' + str(settings['database']['port']),
        'user=' + settings['database']['backend_user'],
        'password=' + settings['database']['backend_password'],
        'dbname=' + settings['database']['name'],
    ))
    connection_url_display = ' '.join((
        'host=' + settings['database']['host'],
        'port=' + str(settings['database']['port']),
        'user=' + settings['database']['backend_user'],
        'dbname=' + settings['database']['name'],
    ))

    print 'PGObserver testdata generator:'
    print '=============================='
    print ''
    print 'Setting connection string to ... ' + connection_url_display
    print ''
    print 'Will create ' + str(args.gh) + ' hosts'
    print 'with ' + str(args.gt) + ' tables'
    print 'with ' + str(args.gp) + ' stored procedures'
    print 'with ' + str(args.gd) + ' days of data'
    print 'with data points every ' + str(args.gi) + ' minutes'
    print ''
    print 'Sleeping for 3s ...'
    time.sleep(3)

    generate_test_data(connection_url, args.gh, args.gt, args.gp, args.gd, args.gi)

    print 'Done'


if __name__ == '__main__':
    main()
