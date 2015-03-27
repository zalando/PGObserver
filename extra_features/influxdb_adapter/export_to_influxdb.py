from argparse import ArgumentParser
from datetime import datetime, timedelta
import logging
import os
import collections
from influxdb import influxdb08 as influxdb
import yaml
import time
import datadb

DEFAULT_CONF_FILE = './influx_config.yaml'
PGO_DATA_SCHEMA = 'monitor_data'
TEMPLATES_FOLDER = 'data_collection_queries'
DATA_COLLECTION_QUERIES_TO_SERIES_MAPPING = [   # tpl_* files are queries with placeholders for host and time range
    ('avg_query_runtime_per_db.sql', {'base_name': 'avg_query_runtime.{ui_shortname}'}),
    ('avg_sproc_runtime_per_db.sql', {'base_name': 'avg_sproc_runtime.{ui_shortname}'}),
    ('avg_sproc_runtime_per_schema.sql', {'base_name': 'avg_sproc_runtime_schema.{ui_shortname}', 'cols_to_expand': ['schema']}),
    ('blocked_process_counts.sql', {'base_name': 'blocked_process_counts.{ui_shortname}'}),
    ('db_general_info.sql', {'base_name': 'db_general.{ui_shortname}'}),
    ('db_size.sql', {'base_name': 'db_size.{ui_shortname}'}),
    ('index_details.sql', {'base_name': 'index_stats.{ui_shortname}',
                                            'cols_to_expand': ['schema', 'index']}),
    ('load.sql', {'base_name': 'load.{ui_shortname}'}),
    ('scan_and_iud_rates_per_db.sql', {'base_name': 'scan_and_iud_rates.{ui_shortname}'}),
    ('scan_and_iud_rates_per_schema.sql', {'base_name': 'scan_and_iud_rates_schema.{ui_shortname}',
                                            'cols_to_expand': ['schema']}),
    ('sproc_details_per_schema_sproc.sql', {'base_name': 'sproc_details.{ui_shortname}',
                                                        'cols_to_expand': ['schema', 'sproc']}),
    ('table_and_index_sizes_per_schema.sql', {'base_name': 'table_and_index_sizes.{ui_shortname}',
                                            'cols_to_expand': ['schema']}),
    ('table_details.sql', {'base_name': 'table_details.{ui_shortname}',
                                            'cols_to_expand': ['schema', 'table']}),
    ('table_io_details.sql', {'base_name': 'table_io_details.{ui_shortname}',
                                            'cols_to_expand': ['schema', 'table']}),
]
MAX_DAYS_TO_SELECT_AT_A_TIME = 7    # chunk size for cases when we need to build up a history of many months
SAFETY_SECONDS_FOR_LATEST_DATA = 30     # let's leave the freshest data out as the whole dataset might not be fully inserted yet


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

    sql = open(os.path.join(TEMPLATES_FOLDER, view_name)).read()
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
    chunk_size = 1000
    i = 0
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
    while i < data_len:
        idb_write_points(db=idb, name=name, columns=columns_filtered, datapoints=load_data[i:i+chunk_size])
        i += chunk_size
    logging.debug('Done in %s seconds', round(time.time() - start_time))


def main():
    parser = ArgumentParser(description='PGObserver InfluxDB Exporter Daemon')
    parser.add_argument('-c', '--config', help='Path to config file. (default: {})'.format(DEFAULT_CONF_FILE),
                        default=DEFAULT_CONF_FILE)
    parser.add_argument('--hosts-to-sync', help='only given host_ids (comma separated) will be pushed to Influx')
    parser.add_argument('--drop-db', help='start with a fresh InfluxDB', action='store_true')
    parser.add_argument('--drop-series', help='drop single series', action='store_true')
    parser.add_argument('--daemon', help='keep scanning for new data in an endless loop', action='store_true')
    parser.add_argument('--check-interval', help='min. seconds between checking for fresh data on PgO for host/view',
                        default=30, type=int)
    group1 = parser.add_mutually_exclusive_group()
    group1.add_argument('-v', '--verbose', help='more chat', action='store_true')
    group1.add_argument('-d', '--debug', help='even more chat', action='store_true')

    args = parser.parse_args()

    logging.basicConfig(format='%(message)s', level=(logging.DEBUG if args.debug
                                                     else (logging.INFO if args.verbose else logging.ERROR)))
    args.config = os.path.expanduser(args.config)

    settings = None
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

    datadb.setConnectionString(conn_string)

    idb = influxdb.InfluxDBClient(settings['influxdb']['host'],
                                 settings['influxdb']['port'],
                                 settings['influxdb']['username'],
                                 settings['influxdb']['password'])

    idb_ensure_database(idb, settings['influxdb']['database'], args.drop_db)
    idb.switch_database(settings['influxdb']['database'])

    logging.debug('DBs found from InfluxDB: %s', idb.get_list_database())
    logging.info('Following views will be synced: %s', [x[0] for x in DATA_COLLECTION_QUERIES_TO_SERIES_MAPPING])

    last_check_time_per_host_and_view = collections.defaultdict(dict)
    loop_counter = 0
    while True:

        loop_counter += 1
        sql_active_hosts = "select host_id as id, lower(host_ui_shortname).replace('-','_') as ui_shortname from hosts where host_enabled order by 2"
        active_hosts, cols = datadb.executeAsDict(sql_active_hosts)
        logging.debug('Nr of active hosts found: %s', len(active_hosts))

        for ah in active_hosts:
            if args.hosts_to_sync:
                if str(ah['id']) not in args.hosts_to_sync.split(','):
                    # logging.debug('Skipping host %s (host_id=%s)', ah['ui_shortname'], ah['id'])
                    continue

            logging.info('Doing host: %s', ah['ui_shortname'])
            host_processing_start_time = time.time()

            for view_name, series_mapping_info in DATA_COLLECTION_QUERIES_TO_SERIES_MAPPING:

                base_name = series_mapping_info['base_name'].format(ui_shortname=ah['ui_shortname'], id=ah['id'])
                is_fan_out = series_mapping_info.get('cols_to_expand', False)
                if args.drop_series and loop_counter == 1:
                    logging.debug('Dropping base series: %s ...', base_name)
                    if is_fan_out:
                        data = idb.query("list series /{}.*/".format(base_name))
                        if data[0]['points']:
                            series = [x['points'][0][1] for x in data]
                            for s in series:
                                logging.debug('Dropping series: %s ...', s)
                                idb.delete_series(s)
                        else:
                            logging.info('No existing series found to delete')
                    else:
                        idb.delete_series(base_name)

                last_data_pull_time_for_view = (last_check_time_per_host_and_view[ah['id']]).get(base_name)
                if last_data_pull_time_for_view > time.time() - args.check_interval:
                    logging.debug('Not pulling data as args.check_interval not passed yet [%s]', base_name)
                    continue
                logging.debug('Fetching data from view "%s" into base series "%s"', view_name, base_name)

                latest_timestamp_for_series = None
                if not (args.drop_series and loop_counter == 1):  # no point to check if series was re-created
                    latest_timestamp_for_series = idb_get_last_timestamp_for_series_as_local_datetime(idb,
                                                                                                      base_name,
                                                                                                      is_fan_out)
                    logging.debug('Latest_timestamp_for_series: %s', latest_timestamp_for_series)
                data, columns = pgo_get_data_and_columns_from_view(ah['id'],
                                                                   view_name,
                                                                   settings['influxdb']['max_days_to_fetch'],
                                                                   latest_timestamp_for_series)
                logging.info('%s rows fetched from view "%s" [ latest prev. timestamp in InfluxDB : %s]', len(data),
                             view_name, latest_timestamp_for_series)
                last_check_time_per_host_and_view[ah['id']][base_name] = time.time()

                try:
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
                                    idb_push_data(idb, prev_row_series_name, columns, data[start_index:current_index],
                                                  expanded_column_indexes)  # expanded_columns_will be removed from the dataset
                                    start_index = current_index
                                current_index += 1
                                prev_row_series_name = series_name

                            idb_push_data(idb, series_name, columns, data[start_index:current_index],
                                                  expanded_column_indexes)
                        else:
                            idb_push_data(idb, series_name, columns, data)

                    else:
                        logging.debug('no fresh data found on PgO')

                except Exception as e:
                    logging.error('ERROR - Could not process %s: %s', view_name, e.message)

            logging.info('Finished processing %s in %ss', ah['ui_shortname'], round(time.time() - host_processing_start_time))

        if not args.daemon:
            break

        time.sleep(1)


if __name__ == '__main__':
    main()