from Queue import Queue
from argparse import ArgumentParser
from datetime import datetime, timedelta
import logging
import os
import collections
import threading
from influxdb import influxdb08 as influxdb
import yaml
import time
import datadb
import traceback

DEFAULT_CONF_FILE = './influx_config.yaml'
PGO_DATA_SCHEMA = 'monitor_data'
TEMPLATES_FOLDER = 'data_collection_queries'
DATA_COLLECTION_QUERIES_TO_SERIES_MAPPING = [   # tpl_* files are queries with placeholders for host and time range
    ('avg_query_runtime_per_db', {'base_name': 'avg_query_runtime.{ui_shortname}'}),
    ('avg_sproc_runtime_per_db', {'base_name': 'avg_sproc_runtime.{ui_shortname}'}),
    ('avg_sproc_runtime_per_schema', {'base_name': 'avg_sproc_runtime_schema.{ui_shortname}', 'cols_to_expand': ['schema']}),
    ('blocked_process_counts', {'base_name': 'blocked_process_counts.{ui_shortname}'}),
    ('db_general_info', {'base_name': 'db_general.{ui_shortname}'}),
    ('db_size', {'base_name': 'db_size.{ui_shortname}'}),
    ('index_details', {'base_name': 'index_stats.{ui_shortname}',
                                            'cols_to_expand': ['schema', 'index']}),
    ('load', {'base_name': 'load.{ui_shortname}'}),
    ('scan_and_iud_rates_per_db', {'base_name': 'scan_and_iud_rates.{ui_shortname}'}),
    ('scan_and_iud_rates_per_schema', {'base_name': 'scan_and_iud_rates_schema.{ui_shortname}',
                                            'cols_to_expand': ['schema']}),
    ('sproc_details_per_schema_sproc', {'base_name': 'sproc_details.{ui_shortname}',
                                                        'cols_to_expand': ['schema', 'sproc']}),
    ('table_and_index_sizes_per_schema', {'base_name': 'table_and_index_sizes.{ui_shortname}',
                                            'cols_to_expand': ['schema']}),
    ('table_details', {'base_name': 'table_details.{ui_shortname}',
                                            'cols_to_expand': ['schema', 'table']}),
    ('table_io_details', {'base_name': 'table_io_details.{ui_shortname}',
                                            'cols_to_expand': ['schema', 'table']}),
]
MAX_DAYS_TO_SELECT_AT_A_TIME = 7    # chunk size for cases when we need to build up a history of many months
SAFETY_SECONDS_FOR_LATEST_DATA = 10     # let's leave the freshest data out as the whole dataset might not be fully inserted yet
settings = None   # for config file contents


def pgo_get_data_and_columns_from_view(host_id, view_name, max_days_to_fetch, idb_latest_timestamp=None):
    dt_now = datetime.now()
    from_timestamp = idb_latest_timestamp
    to_timestamp = dt_now

    if from_timestamp is None:
        from_timestamp = dt_now - timedelta(days=max_days_to_fetch)

    if from_timestamp < dt_now - timedelta(days=MAX_DAYS_TO_SELECT_AT_A_TIME):
        to_timestamp = from_timestamp + timedelta(days=MAX_DAYS_TO_SELECT_AT_A_TIME)
    else:
        to_timestamp = to_timestamp - timedelta(seconds=SAFETY_SECONDS_FOR_LATEST_DATA)

    if from_timestamp >= to_timestamp:
        return [], None

    sql = open(os.path.join(TEMPLATES_FOLDER, view_name + '.sql')).read()
    sql_params = {'host_id': host_id, 'from_timestamp': from_timestamp, 'to_timestamp': to_timestamp}

    logging.debug("Executing:")
    logging.debug("%s", datadb.mogrify(sql, sql_params))

    view_data, columns = datadb.execute(sql, sql_params)

    # removing timestamp, we only want to store the utc epoch "time" column
    timestamp_index = columns.index('timestamp')
    if timestamp_index != 0:
        raise Exception('"timestamp" needs to be the 1st column returned!')
    columns.remove('timestamp')

    ret_data = []
    for d in view_data:
        ret_data.append(list(d[1:]))

    return ret_data, columns


def get_idb_client():
    idb_client = influxdb.InfluxDBClient(
        settings['influxdb']['host'],
        settings['influxdb']['port'],
        settings['influxdb']['username'],
        settings['influxdb']['password'],
        settings['influxdb']['database'])
    return idb_client


def idb_write_points(db, name, columns, datapoints):
    if len(columns) != len(datapoints[0]):
        raise Exception('Inequal inputs')
    data = [{
        "name": name,
        "columns": columns,
        "points": datapoints
    }]
    db.write_points(data, time_precision='s', batch_size=1000)


def idb_ensure_database(db, dbname, recreate=None):
    db_names = [x['name'] for x in db.get_list_database()]
    if recreate and dbname in db_names:
        logging.info('Recreating DB %s on InfluxDB...', dbname)
        db.delete_database(dbname)
    if dbname not in [x['name'] for x in db.get_list_database()]:
        db.create_database(dbname)


def idb_get_last_timestamp_for_series_as_local_datetime(db, series_name, is_fan_out=False):
    """ Influx times are UTC, convert to local """
    max_as_datetime = None
    sql = 'select * from {} limit 1'.format(series_name)
    if is_fan_out:  # need to loop over all hosts to find the max_timestamp
        sql = 'select * from /{}.*/ limit 1'.format(series_name)

    try:
        logging.debug('Executing "%s" on InfluxDB', sql)
        data = db.query(sql, time_precision='s')
        if data:
            # logging.debug('Latest data for series %s: %s', series_name, data)
            max_timestamp = None
            for x in data:
                if x['points'][0][0] > max_timestamp:
                    max_timestamp = x['points'][0][0]
            max_as_datetime = datetime.fromtimestamp(max_timestamp + 1)     # adding 1s to safeguard against epoch to datetime conversion jitter
    except Exception as e:
        logging.warning("Exception while getting latest timestamp: %s", e.message)

    return max_as_datetime


def idb_push_data(idb, name, columns, load_data, column_indexes_to_skip=[]):
    if not load_data:
        return
    data_len = len(load_data)
    columns_filtered = list(columns)

    if column_indexes_to_skip:
        for d in load_data:
            skip_counter = 0
            for i_skip in column_indexes_to_skip:
                d.pop(i_skip - skip_counter)
                skip_counter += 1
        skip_counter = 0
        for i_skip in column_indexes_to_skip:
            columns_filtered.pop(i_skip - skip_counter)
            skip_counter += 1

    logging.debug('Pushing %s data points to InfluxDB for series %s...', data_len, name)
    start_time = time.time()
    idb_write_points(db=idb, name=name, columns=columns_filtered, datapoints=load_data)
    logging.debug('Done in %s seconds', round(time.time() - start_time))

queue = Queue()


class WorkerThread(threading.Thread):
    """ Waits to fetch the next host to be synced from the que and then pulls data from PgO and pushes it to InfluxDB"""

    def __init__(self, args):
        self.host_data = None
        self.idb_client = None
        self.args = args
        super(WorkerThread, self).__init__()
        self.daemon = True

    def run(self):
        logging.info('Starting thread')
        while True:
            data = queue.get()
            logging.info('Refresh command received for %s', data['ui_shortname'])
            try:
                self.idb_client = get_idb_client()

                do_pull_push_for_one_host(data['id'], data['ui_shortname'], data['is_first_loop'],
                                          self.args, self.idb_client)
            except Exception as e:
                logging.error('ERROR in worker thread: %s', e)
                logging.error('%s', traceback.format_exc())


def do_pull_push_for_one_host(host_id, ui_shortname, is_first_loop, args, influx_conn):
            try:
                logging.info('Doing host: %s', ui_shortname)
                host_processing_start_time = time.time()

                for view_name, series_mapping_info in DATA_COLLECTION_QUERIES_TO_SERIES_MAPPING:
                    if view_name not in settings['data_collection_queries_to_process']:
                        continue

                    base_name = series_mapping_info['base_name'].format(ui_shortname=ui_shortname, id=host_id)
                    is_fan_out = series_mapping_info.get('cols_to_expand', None)

                    if args.drop_series and is_first_loop:
                        logging.debug('Dropping base series: %s ...', base_name)
                        if is_fan_out:
                            data = influx_conn.query("list series /{}.*/".format(base_name))
                            if data[0]['points']:
                                series = [x['points'][0][1] for x in data]
                                for s in series:
                                    logging.debug('Dropping series: %s ...', s)
                                    influx_conn.delete_series(s)
                            else:
                                logging.info('No existing series found to delete')
                        else:
                            influx_conn.delete_series(base_name)

                    logging.debug('Fetching data from view "%s" into base series "%s"', view_name, base_name)

                    latest_timestamp_for_series = None
                    if not (args.drop_series and is_first_loop):  # no point to check if series was re-created
                        latest_timestamp_for_series = idb_get_last_timestamp_for_series_as_local_datetime(influx_conn,
                                                                                                          base_name,
                                                                                                          is_fan_out)
                        logging.debug('Latest_timestamp_for_series: %s', latest_timestamp_for_series)
                    data, columns = pgo_get_data_and_columns_from_view(host_id,
                                                                       view_name,
                                                                       settings['influxdb']['max_days_to_fetch'],
                                                                       latest_timestamp_for_series)
                    logging.info('%s rows fetched from view "%s" [ latest prev. timestamp in InfluxDB : %s]', len(data),
                                 view_name, latest_timestamp_for_series)

                    if len(data) > 0:
                        series_name = base_name
                        if is_fan_out:          # could leave it to continuous queries also but it would mean data duplication
                            prev_row_series_name = None
                            expanded_column_indexes = []
                            start_index = 0
                            current_index = 0
                            # logging.debug("Columns to expand: %s", series_mapping_info['cols_to_expand'])
                            for col in series_mapping_info['cols_to_expand']:
                                expanded_column_indexes.append(columns.index(col))
                            for row in data:
                                series_name = base_name
                                for ci in expanded_column_indexes:
                                    series_name += '.' + str(row[ci])
                                if series_name != prev_row_series_name and prev_row_series_name:
                                    idb_push_data(influx_conn, prev_row_series_name, columns, data[start_index:current_index],
                                                  expanded_column_indexes)  # expanded_columns_will be removed from the dataset
                                    start_index = current_index
                                current_index += 1
                                prev_row_series_name = series_name

                            idb_push_data(influx_conn, series_name, columns, data[start_index:current_index],
                                                  expanded_column_indexes)
                        else:
                            idb_push_data(influx_conn, series_name, columns, data)

                    else:
                        logging.debug('no fresh data found on PgO')

            except Exception as e:
                logging.error('ERROR - Could not process %s: %s', view_name, e.message)

            logging.info('Finished processing %s in %ss', ui_shortname, round(time.time() - host_processing_start_time))


def main():
    parser = ArgumentParser(description='PGObserver InfluxDB Exporter Daemon')
    parser.add_argument('-c', '--config', help='Path to config file. (default: {})'.format(DEFAULT_CONF_FILE),
                        default=DEFAULT_CONF_FILE)
    parser.add_argument('--hosts-to-sync', help='only given host_ids (comma separated) will be pushed to Influx')
    parser.add_argument('--drop-db', action='store_true', help='start with a fresh InfluxDB. Needs root login i.e. meant for testing purposes')
    parser.add_argument('--drop-series', action='store_true', help='drop single series')
    parser.add_argument('--check-interval', help='min. seconds between checking for fresh data on PgO for host/view',
                        default=30, type=int)
    group1 = parser.add_mutually_exclusive_group()
    group1.add_argument('-v', '--verbose', action='store_true', help='more chat')
    group1.add_argument('-d', '--debug', action='store_true', help='even more chat')

    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s %(threadName)s %(message)s', level=(logging.DEBUG if args.debug
                                                     else (logging.INFO if args.verbose else logging.ERROR)))
    args.config = os.path.expanduser(args.config)

    global settings
    if os.path.exists(args.config):
        logging.info("Trying to read config file from %s", args.config)
        with open(args.config, 'rb') as fd:
            settings = yaml.load(fd)

    if settings is None:
        logging.error('Config file missing - Yaml file could not be found')
        parser.print_help()
        exit(1)

    conn_string = ' '.join((
        'dbname=' + settings['database']['name'],
        'host=' + settings['database']['host'],
        'user=' + settings['database']['frontend_user'],
        'port=' + str(settings['database']['port']),
    ))

    logging.info('Setting connection string to: %s', conn_string)

    conn_string = ' '.join((
        'dbname=' + settings['database']['name'],
        'host=' + settings['database']['host'],
        'user=' + settings['database']['frontend_user'],
        'password=' + settings['database']['frontend_password'],
        'port=' + str(settings['database']['port']),
    ))

    datadb.set_connection_string(conn_string)

    idb = influxdb.InfluxDBClient(settings['influxdb']['host'],
                                 settings['influxdb']['port'],
                                 settings['influxdb']['username'],
                                 settings['influxdb']['password'])

    if args.drop_db:
        logging.debug('DBs found from InfluxDB: %s', idb.get_list_database())
        idb_ensure_database(idb, settings['influxdb']['database'], args.drop_db)

    idb.switch_database(settings['influxdb']['database'])

    logging.info('Following views will be synced: %s', [x[0] for x in DATA_COLLECTION_QUERIES_TO_SERIES_MAPPING])

    hosts_to_sync = []
    if args.hosts_to_sync:
        hosts_to_sync = args.hosts_to_sync.split(',')
        hosts_to_sync = [int(x) for x in hosts_to_sync]
        logging.debug('Syncing only hosts: %s', hosts_to_sync)

    last_queued_time_for_host = collections.defaultdict(dict)
    loop_counter = 0
    workers = []
    active_hosts = []
    active_hosts_refresh_time = 0
    sql_active_hosts = "select host_id as id, replace(lower(host_ui_shortname), '-','_') as ui_shortname from monitor_data.hosts " \
                       "where host_enabled and (%s = '{}' or host_id = any(%s)) order by 2"

    while True:

        if time.time() - active_hosts_refresh_time > 180:  # checking for hosts changes every 3 minutes
            active_hosts, cols = datadb.executeAsDict(sql_active_hosts, (hosts_to_sync, hosts_to_sync))
            active_hosts_refresh_time = time.time()

        if loop_counter == 0:   # setup
            workers_to_spawn = min(min(len(hosts_to_sync) if hosts_to_sync else settings['max_worker_threads'], settings['max_worker_threads']), len(sql_active_hosts))
            logging.debug('Nr of monitored hosts: %s', len(active_hosts))
            logging.info('Creating %s worker threads...', workers_to_spawn)
            for i in range(0, workers_to_spawn):
                wt = WorkerThread(args)
                wt.start()
                workers.append(wt)

        for ah in active_hosts:

            last_data_pull_time_for_view = last_queued_time_for_host.get(ah['id'], 0)
            if time.time() - last_data_pull_time_for_view < args.check_interval:
                # logging.debug('Not pulling data as args.check_interval not passed yet [host_id %s]', ah['id'])
                continue

            logging.info('Putting %s to queue...', ah['ui_shortname'])
            queue.put({'id': ah['id'], 'ui_shortname': ah['ui_shortname'], 'is_first_loop': loop_counter == 0})
            last_queued_time_for_host[ah['id']] = time.time()

        logging.debug('Main thread sleeps...')
        time.sleep(5)
        loop_counter += 1

if __name__ == '__main__':
    main()