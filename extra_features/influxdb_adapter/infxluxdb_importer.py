from argparse import ArgumentParser
import datetime
import os
from influxdb import client as influxdb
import yaml
import time
import datadb

DEFAULT_CONF_FILE = './influx_config.yaml'


VIEW_TO_SERIES_MAPPING = {
    'monitor_data.v_influx_load': 'load.{host_ui_shortname}',
    'monitor_data.v_influx_db_info': 'db_info.{host_ui_shortname}',
    'monitor_data.v_influx_sproc_info': 'sproc_info.{host_ui_shortname}',
    'monitor_data.v_influx_table_info': 'table_info.{host_ui_shortname}',
}

# TODO add option to split into separate series based on some columns ?
# zB { 'series': 'sproc_info.{host_ui_shortname}', 'expand_series': 'sproc, month' }


def pgo_get_data_and_columns_from_view(host_id, view_name, max_days_to_fetch, idb_max_timestamp=None):
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
    idb_max_timestamp_as_datetime = None

    if idb_max_timestamp:       # idb_max_timestamp is Epoch UTC, convert to local datetime
        idb_max_timestamp_as_datetime = datetime.fromtimestamp(idb_max_timestamp)

    print "executing..."
    print datadb.mogrify(sql, (host_id, max_days_to_fetch, idb_max_timestamp_as_datetime, idb_max_timestamp_as_datetime))
    view_data, columns = datadb.executeAsDict(sql, (host_id, max_days_to_fetch, idb_max_timestamp_as_datetime, idb_max_timestamp_as_datetime))
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
    db.write_points_with_precision(data, time_precision='s')


def idb_ensure_database(db, dbname, recreate=None):
    if recreate:
        print 'Recreating', dbname, 'on InfluxDB...'
        db.delete_database(dbname)
    if dbname not in [x['name'] for x in db.get_database_list()]:
        db.create_database(dbname)


def idb_get_last_timestamp_for_series_as_local_datetime(db, series_name):
    """ Influx times are UTC, convert to local """
    max_as_datetime = None

    try:
        max_timestamp = db.query("""select * from """ + series_name + """ limit 1""")[0]['points'][0][0]
        max_as_datetime = datetime.datetime.fromtimestamp(max_timestamp)
    except Exception as e:
        print e

    return max_as_datetime


def idb_push_data(idb, name, columns, load_data):
    data_len = len(load_data)
    chunk_size = 500
    i = 0
    while i < data_len:
        idb_write_points(db=idb, name=name, columns=columns, datapoints=load_data[i:i+chunk_size])
        i += chunk_size


def main():
    parser = ArgumentParser(description='PGObserver InfluxDB Exporter Daemon')
    parser.add_argument('-c', '--config', help='Path to config file. (default: %s)'.format(DEFAULT_CONF_FILE),
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
    idb.switch_db(settings['influxdb']['database'])

    print 'DBs found from InfluxDB:', idb.get_database_list()

    print 'following views will be synced', VIEW_TO_SERIES_MAPPING.keys()

    loop_counter = 0
    while True:

        loop_counter += 1
        print 'doing loop', loop_counter
        active_hosts, cols = datadb.executeAsDict('select host_id, host_ui_shortname from hosts where host_enabled order by 1')
        print 'active hosts found', len(active_hosts)

        for active_host in active_hosts:
            if args.hosts_to_sync:
                if str(active_host['host_id']) not in args.hosts_to_sync.split(','):
                    print 'skipping host', active_host
                    continue

            print 'doing host:', active_host['host_ui_shortname']

            for view_name, series_format in VIEW_TO_SERIES_MAPPING.iteritems():
                series_name = series_format.format(host_id=active_host['host_id'],
                                                   host_ui_shortname=active_host['host_ui_shortname'])
                if args.drop_series and loop_counter == 1:
                    print 'dropping series:', series_name, '...'
                    idb.delete_series(series_name)

                print 'fetching data from view ', view_name, 'into series', series_name

                latest_timestamp_for_series = None
                if not args.drop_series and loop_counter == 1:  # no point to check if series was re-created
                    latest_timestamp_for_series = idb_get_last_timestamp_for_series_as_local_datetime(idb, series_name)
                data, columns = pgo_get_data_and_columns_from_view(active_host['host_id'],
                                                                   view_name,
                                                                   settings['influxdb']['max_days_to_fetch'],
                                                                   latest_timestamp_for_series)
                # print data, columns
                if len(data) > 0:
                    start_time = time.time()
                    print 'pushing', len(data), 'data points to InfluxDB...'
                    idb_push_data(idb, series_name, columns, data)
                    print 'done in ', time.time() - start_time, 'seconds'
                else:
                    print 'no fresh data found on PgO'

            print 'finished processing ', active_host['host_ui_shortname']

        if not args.daemon:
            break

        print 'sleeping', args.check_interval, 's before fetching new data'
        time.sleep(args.check_interval)


if __name__ == '__main__':
    main()