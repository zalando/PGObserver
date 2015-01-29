from argparse import ArgumentParser
from datetime import datetime
import json
import os
from influxdb import client as influxdb
import yaml
import time
import datadb

DEFAULT_CONF_FILE = './influx_config.yaml'


VIEW_TO_SERIES_MAPPING = {
    # 'monitor_data.v_influx_load': 'load.{host_ui_shortname}',
    # 'monitor_data.v_influx_db_info': 'db_info.{host_ui_shortname}',
    # 'monitor_data.v_influx_sproc_info': 'sproc_info.{host_ui_shortname}',
    # 'monitor_data.v_influx_table_info': 'table_info.{host_ui_shortname}',
    # 'monitor_data.v_influx_table_io_info': 'table_io_info.{host_ui_shortname}',
    # 'monitor_data.v_influx_index_info': 'index_info.{host_ui_shortname}',
    # 'monitor_data.v_influx_blocked_processes': 'blocked_processes.{host_ui_shortname}',

    # views starting with TPL are actually not views but SQL templates
    'tpl_avg_query_runtime.sql': 'avg_query_runtime.{host_ui_shortname}'
}

TEMPLATES_FOLDER = 'data_collection_sql_templates'

# TODO add option to split into separate series based on some columns ?
# zB { 'series': 'sproc_info.{host_ui_shortname}', 'expand_series': 'sproc, month' }


def pgo_get_data_and_columns_from_view(host_id, host_ui_shortname, view_name, max_days_to_fetch, idb_latest_timestamp=None):
    sql = """
        select
          *
        from
          """ + view_name + """
        where
          host_id = %s
          and "timestamp" > current_date - %s
          and case when %s is null then true else "timestamp" > %s end
        order by time
        """
    sql_params = (host_id, max_days_to_fetch, idb_latest_timestamp, idb_latest_timestamp)

    if view_name.startswith('tpl_'):
        sql = open(os.path.join(TEMPLATES_FOLDER, view_name)).read()
        # if not idb_latest_timestamp:
        #     idb_latest_timestamp = 'NULL'
        # sql = sql.format(in_host_id=host_id, in_last_timestamp="'"+idb_latest_timestamp+"'", in_max_days=max_days_to_fetch)
        sql_params = {'host_id': host_id, 'last_timestamp': idb_latest_timestamp, 'max_days': max_days_to_fetch}
        # print sql

    print "executing..."
    print datadb.mogrify(sql, sql_params)
    view_data, columns = datadb.executeAsDict(sql, sql_params)
    # print len(view_data), 'points found'
    # print view_data[0]

    # removing timestamp + host_id, they're needed only for efficiently fetching data from the views
    columns.remove('timestamp')
    columns.remove('host_id')

    ret_data = []
    for d in view_data:
        one_row = []
        for c in columns:
            one_row.append(d[c])
        ret_data.append(one_row)

    return ret_data, columns


def idb_write_points(db, name, columns, datapoints):
    if len(columns) != len(datapoints[0]):
        raise Exception('Inequal inputs')
    data = [{
        "name": name,
        "columns": columns,
        "points": datapoints
    }]
    db.write_points(data, time_precision='s')


def idb_ensure_database(db, dbname, recreate=None):
    if recreate:
        print 'Recreating', dbname, 'on InfluxDB...'
        db.delete_database(dbname)
    if dbname not in [x['name'] for x in db.get_list_database()]:
        db.create_database(dbname)


def idb_get_last_timestamp_for_series_as_local_datetime(db, series_name):
    """ Influx times are UTC, convert to local """
    max_as_datetime = None

    try:
        data = db.query("""select * from """ + series_name + """ limit 1""", time_precision='s')[0]['points'][0]
        # print data
        max_timestamp = data[0]
        max_as_datetime = datetime.fromtimestamp(max_timestamp + 1) # adding 1s
    except Exception as e:
        print e
    return max_as_datetime


def idb_push_data(idb, name, columns, load_data):
    data_len = len(load_data)
    chunk_size = 1000
    i = 0
    while i < data_len:
        idb_write_points(db=idb, name=name, columns=columns, datapoints=load_data[i:i+chunk_size])
        i += chunk_size


def main():
    parser = ArgumentParser(description='PGObserver InfluxDB Exporter Daemon')
    parser.add_argument('-c', '--config', help='Path to config file. (default: {})'.format(DEFAULT_CONF_FILE),
                        default=DEFAULT_CONF_FILE)
    parser.add_argument('--hosts-to-sync', help='only given host_ids (comma separated) will be pushed to Influx')
    parser.add_argument('--drop-db', help='start with a fresh InfluxDB', action='store_true')
    parser.add_argument('--drop-series', help='drop single series', action='store_true')
    parser.add_argument('--daemon', help='keep scanning for new data in an endless loop', action='store_true')
    parser.add_argument('--check-interval', help='seconds to sleep before re-looping the PgO hosts for new data',
                        default=30, type=int)

    args = parser.parse_args()

    args.config = os.path.expanduser(args.config)

    settings = None
    if os.path.exists(args.config):
        print "trying to read config file from {}".format(args.config)
        with open(args.config, 'rb') as fd:
            settings = yaml.load(fd)

    if settings is None:
        print 'Config file missing - Yaml file could not be found'
        parser.print_help()
        exit(1)

    conn_string = ' '.join((
        'dbname=' + settings['database']['name'],
        'host=' + settings['database']['host'],
        'user=' + settings['database']['frontend_user'],
        'port=' + str(settings['database']['port']),
    ))

    print 'Setting connection string to ... ' + conn_string

    conn_string = ' '.join((
        'dbname=' + settings['database']['name'],
        'host=' + settings['database']['host'],
        'user=' + settings['database']['frontend_user'],
        'password=' + settings['database']['frontend_password'],
        'port=' + str(settings['database']['port']),
    ))

    datadb.setConnectionString(conn_string)

    idb = influxdb.InfluxDBClient(settings['influxdb']['host'],
                                 settings['influxdb']['port'],
                                 settings['influxdb']['username'],
                                 settings['influxdb']['password'])

    idb_ensure_database(idb, settings['influxdb']['database'], args.drop_db)
    idb.switch_database(settings['influxdb']['database'])

    print 'DBs found from InfluxDB:', idb.get_list_database()

    print 'following views will be synced', VIEW_TO_SERIES_MAPPING.keys()

    loop_counter = 0
    while True:

        loop_counter += 1
        print 'doing loop', loop_counter
        active_hosts, cols = datadb.executeAsDict('select host_id, lower(host_ui_shortname) as host_ui_shortname from hosts where host_enabled order by 1')
        print 'active hosts found', len(active_hosts)

        for active_host in active_hosts:
            if args.hosts_to_sync:
                if str(active_host['host_id']) not in args.hosts_to_sync.split(','):
                    # print 'skipping host', active_host
                    continue

            print 'doing host:', active_host['host_ui_shortname']
            host_update_marker_set = False

            for view_name, series_format in VIEW_TO_SERIES_MAPPING.iteritems():
                series_name = series_format.format(host_id=active_host['host_id'],
                                                   host_ui_shortname=active_host['host_ui_shortname'])
                if args.drop_series and loop_counter == 1:
                    print 'dropping series:', series_name, '...'
                    idb.delete_series(series_name)

                print 'fetching data from view ', view_name, 'into series', series_name

                latest_timestamp_for_series = None
                if not (args.drop_series and loop_counter == 1):  # no point to check if series was re-created
                    latest_timestamp_for_series = idb_get_last_timestamp_for_series_as_local_datetime(idb, series_name)
                    print 'latest_timestamp_for_series:', latest_timestamp_for_series
                data, columns = pgo_get_data_and_columns_from_view(active_host['host_id'],
                                                                   active_host['host_ui_shortname'],
                                                                   view_name,
                                                                   settings['influxdb']['max_days_to_fetch'],
                                                                   latest_timestamp_for_series)
                # print data, columns
                if len(data) > 0:
                    start_time = time.time()
                    print 'pushing', len(data), 'data points to InfluxDB [', series_name, ']...'
                    idb_push_data(idb, series_name, columns, data)
                    print 'done in ', time.time() - start_time, 'seconds'
                    # insert "last update" marker into special series "hosts". useful for listing all different hosts for templated queries
                    if not host_update_marker_set:
                        idb_push_data(idb, 'hosts', ['host'], [(active_host['host_ui_shortname'],)])
                        host_update_marker_set = True
                else:
                    print 'no fresh data found on PgO'

            print 'finished processing ', active_host['host_ui_shortname']

        if not args.daemon:
            break

        print 'sleeping', args.check_interval, 's before fetching new data'
        time.sleep(args.check_interval)


if __name__ == '__main__':
    main()