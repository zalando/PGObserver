from Queue import Queue
from argparse import ArgumentParser
from datetime import datetime, timedelta
from influxdb.exceptions import InfluxDBClientError
import logging
import os
import collections
import threading
import boto
import influxdb
import itertools
import yaml
import time
import datadb
import traceback


DEFAULT_CONF_FILE = './influx_config.yaml'
PGO_DATA_SCHEMA = 'monitor_data'
TEMPLATES_FOLDER = 'data_collection_queries'
DATA_COLLECTION_QUERIES_TO_SERIES_MAPPING = {       # queries are located in the "data_collection_queries" folder
    'avg_query_runtime_per_db': {'base_name': 'avg_query_runtime.{ui_shortname}'},
    'avg_sproc_runtime_per_db': {'base_name': 'avg_sproc_runtime.{ui_shortname}'},
    'avg_sproc_runtime_per_schema': {'base_name': 'avg_sproc_runtime_schema.{ui_shortname}', 'cols_to_expand': ['schema']},
    'blocked_process_counts': {'base_name': 'blocked_process_counts.{ui_shortname}'},
    'db_general_info': {'base_name': 'db_general.{ui_shortname}'},
    'db_size': {'base_name': 'db_size.{ui_shortname}'},
    'index_details': {'base_name': 'index_stats.{ui_shortname}',
                                            'cols_to_expand': ['schema', 'index']},
    'load': {'base_name': 'load.{ui_shortname}'},
    'scan_and_iud_rates_per_db': {'base_name': 'scan_and_iud_rates.{ui_shortname}'},
    'scan_and_iud_rates_per_schema': {'base_name': 'scan_and_iud_rates_schema.{ui_shortname}',
                                            'cols_to_expand': ['schema']},
    'sproc_details_per_schema_sproc': {'base_name': 'sproc_details.{ui_shortname}',
                                                        'cols_to_expand': ['schema', 'sproc']},
    'table_and_index_sizes_per_schema': {'base_name': 'table_and_index_sizes.{ui_shortname}',
                                            'cols_to_expand': ['schema']},
    'table_details': {'base_name': 'table_details.{ui_shortname}',
                                            'cols_to_expand': ['schema', 'table']},
    'table_io_details': {'base_name': 'table_io_details.{ui_shortname}',
                                            'cols_to_expand': ['schema', 'table']},
    }

settings = {}   # for config file contents


def pgo_get_data_and_columns_from_view(host_id, ui_shortname, view_name, max_days_to_fetch, idb_latest_timestamp=None):
    dt_now = datetime.now()
    from_timestamp = idb_latest_timestamp
    to_timestamp = dt_now

    if from_timestamp is None:
        from_timestamp = dt_now - timedelta(days=max_days_to_fetch)

    if from_timestamp < dt_now - timedelta(days=settings.get('max_days_to_select_at_a_time', 3)):
        to_timestamp = from_timestamp + timedelta(days=settings.get('max_days_to_select_at_a_time', 3))
    else:
        to_timestamp = to_timestamp - timedelta(seconds=settings.get('safety_seconds_for_latest_data', 10))

    if from_timestamp >= to_timestamp:
        return [], None

    sql = open(os.path.join(TEMPLATES_FOLDER, view_name + '.sql')).read()
    sql_params = {'host_id': host_id, 'from_timestamp': from_timestamp, 'to_timestamp': to_timestamp,
                  'lag_interval': settings.get('lag_interval', '4 hours')}

    logging.debug("Executing:")
    logging.debug("%s", datadb.mogrify(sql, sql_params))

    try:
        view_data, columns = datadb.executeAsDict(sql, sql_params)
        return view_data, columns
    except Exception as e:
        logging.error('[%s] could not get data from PGO view %s: %s', ui_shortname, view_name, e)

    return [], []


def get_idb_client():
    idb_client = influxdb.InfluxDBClient(
        settings['influxdb']['host'],
        settings['influxdb']['port'],
        settings['influxdb']['username'],
        settings['influxdb']['password'],
        settings['influxdb']['database'])
    return idb_client


def idb_write_points(ui_shortname, measurement, column_names, tag_names, data_by_tags):
    dataset = []

    if 'timestamp' in column_names:     # not storing human readable timestamp, useful for debugging only
        column_names.remove('timestamp')
    column_names.remove('time')

    for tag_dict, data in data_by_tags:
        if set(column_names) - set(data[0].keys()):
            logging.error('columns: %s', column_names)
            logging.error('data[0]: %s', data[0])
            raise Exception('Some required columns missing!')

        for d in data:
            field_data = dict((x[0], x[1]) for x in d.iteritems() if x[0] in column_names and x[0] not in tag_names)
            dataset.append({
                "measurement": settings['influxdb']['tag_mode_series_prefix'] + measurement,
                "time": d['time'],  # epoch_seconds
                "tags": tag_dict,
                "fields": field_data
            })

    db = get_idb_client()
    logging.debug('[%s] pushing %s data points to InfluxDB in "tag mode" for measurement "%s" ...', ui_shortname,
                  len(dataset), measurement)
    logging.debug('[%s] data[0] = %s', ui_shortname, dataset[0])
    start_time = time.time()
    db.write_points(dataset, time_precision='s')
    logging.debug('[%s] done in %s seconds', ui_shortname, round(time.time() - start_time))


def idb_ensure_database(db, dbname, recreate=None):
    db_names = [x['name'] for x in db.get_list_database()]
    if recreate and dbname in db_names:
        logging.info('Recreating DB %s on InfluxDB...', dbname)
        db.drop_database(dbname)
        db.create_database(dbname)
    elif dbname not in db_names:
        logging.info('Creating DB %s on InfluxDB...', dbname)
        db.create_database(dbname)

    ret_policies = [x['name'] for x in db.get_list_retention_policies(dbname)]
    if 'pgobserver' not in ret_policies:
        db.create_retention_policy('pgobserver', str(settings['influxdb']['retention_period_days']) + 'd', '1',
                                   dbname, default=True)


def idb_get_last_timestamp_for_series_as_local_datetime(series_name, ui_shortname):
    """ Influx times are UTC, convert to local """
    last_tz_gmt, last_tz_local = last_tz_from_influx.get(series_name+ui_shortname, (None, None))
    date_filter = "'{}'".format(last_tz_gmt) if last_tz_gmt else 'now() - {}d'.format(settings['influxdb']['max_days_to_fetch'])

    sql = "select * from {} where dbname = '{}' and time > {} order by time desc limit 1".format(series_name, ui_shortname, date_filter)

    try:
        db = get_idb_client()
        logging.info('[%s] executing "%s" on InfluxDB', ui_shortname, sql)
        rs = db.query(sql)    # params={'precision': 's'} doesn't seem to have any effect on reading ?
        if rs:
            max_timestamp_gmt = None
            for r in rs:
                series_max = (list(r)[-1])['time']    # 2015-08-20T17:22:21Z
                if series_max > max_timestamp_gmt:
                    max_timestamp_gmt = series_max

            # conversion to local datetime as assuming Postgres DB operates on local tz
            dt = datetime.strptime(max_timestamp_gmt, '%Y-%m-%dT%H:%M:%SZ')
            local_tz_str = time.strftime("%z")     # +0200
            hours = int(local_tz_str[0:3])
            minutes = int(local_tz_str[3:5])        # TODO brr, need to find some proper datelib
            max_as_datetime = dt + timedelta(hours=hours, minutes=minutes, seconds=1)
            last_tz_from_influx[series_name+ui_shortname] = (max_timestamp_gmt, max_as_datetime)
            return max_as_datetime
        elif last_tz_gmt:
            return last_tz_local

    except InfluxDBClientError as idbe:
        if idbe.message.find('database not found') >= 0:
            logging.warning("DB %s not found. Recreating ...", settings['influxdb']['database'])
            idb_ensure_database(db, settings['influxdb']['database'])
    except Exception as e:
        logging.warning("Exception while getting latest timestamp: %s", e.message)

    return None


def split_by_tags_if_needed_and_push_to_influx(measurement, ui_shortname, data, column_names, tags=[]):
    if not data:
        return
    for col in column_names + tags:
        if col not in data[0]:  # checking only 1st row should be enough
            raise Exception('Required columns not existing in data set!')

    mandatory_tag = {'dbname': ui_shortname}     # only mandatory tag
    data_by_tags = []

    if tags:    # creating groups for different tag value sets
        if len(tags) == 1:
            groups = itertools.groupby(data, lambda x: x[tags[0]])
        elif len(tags) == 2:    # TODO is there a nicer dynamic way?
            groups = itertools.groupby(data, lambda x: (x[tags[0]], x[tags[1]]))
        else:
            raise Exception('Max 2 fan-out columns supported currently')

        for group, group_data in groups:
            tags_dict = dict(mandatory_tag)
            # print 'group', group
            if len(tags) == 1:
                tags_dict[tags[0]] = group
            elif len(tags) == 2:
                tags_dict[tags[0]] = group[0]
                tags_dict[tags[1]] = group[1]
            data_by_tags.append((tags_dict, list(group_data)))
    else:   # no tags
        data_by_tags = [(mandatory_tag, data)]
    idb_write_points(ui_shortname, measurement=measurement, column_names=column_names, tag_names=tags, data_by_tags=data_by_tags)


def get_s3_key_as_string(region_name, bucket_name, key_name):
    conn = boto.s3.connect_to_region(region_name)
    bucket = conn.get_bucket(bucket_name=bucket_name)
    if bucket:
        key = bucket.get_key(key_name)
        if key:
            return key.get_contents_as_string()
    logging.error('S3 key not found: %s/%s/%s', region_name, bucket_name, key_name)
    return None


queue = Queue()
last_tz_from_influx = {}    # {'db1series1'=tz,...} max timestamps read back from Influx to only query newer data from Postgres


class WorkerThread(threading.Thread):
    """ Waits to fetch the next host to be synced from the que and then pulls data from PgO and pushes it to InfluxDB"""

    def __init__(self, args):
        self.args = args
        super(WorkerThread, self).__init__()
        self.daemon = True

    def run(self):
        logging.info('Starting thread')
        while True:
            data = queue.get()

            logging.info('Refresh command received for %s', data['ui_shortname'])

            try:
                do_pull_push_for_one_host(data['id'], data['ui_shortname'], data['is_first_loop'],
                                          self.args)
            except Exception as e:
                logging.error('ERROR in worker thread: %s', e)
                logging.error('%s', traceback.format_exc())


def do_pull_push_for_one_host(host_id, ui_shortname, is_first_loop, args):
            try:
                logging.info('Doing host: %s', ui_shortname)
                host_processing_start_time = time.time()

                for view_name in settings['data_collection_queries_to_process']:
                    series_mapping_info = DATA_COLLECTION_QUERIES_TO_SERIES_MAPPING.get(view_name)
                    if not series_mapping_info:
                        raise Exception('Unknown query "{}", mapping to series not found!'.format(view_name))

                    measurement_name = series_mapping_info['base_name'].split('.')[0]
                    tags = series_mapping_info.get('cols_to_expand', [])

                    logging.debug('[%s] trying to fetch latest timestamp from existing series "%s" ...', ui_shortname, measurement_name)
                    latest_timestamp_for_series = idb_get_last_timestamp_for_series_as_local_datetime(measurement_name,
                                                                                                      ui_shortname)
                    logging.debug('[%s] latest_timestamp_for_series: %s', ui_shortname, latest_timestamp_for_series)

                    logging.info('[%s] fetching metrics data from PgO for view : "%s"...', ui_shortname, view_name)
                    data, column_names_from_query = pgo_get_data_and_columns_from_view(host_id, ui_shortname,
                                                                       view_name,
                                                                       settings['influxdb']['max_days_to_fetch'],
                                                                       latest_timestamp_for_series)
                    logging.info('[%s] %s rows fetched from view "%s" [ latest prev. timestamp in InfluxDB : %s]',
                                 ui_shortname, len(data), view_name, latest_timestamp_for_series)
                    for col in column_names_from_query:
                        if col[0].isdigit():
                            raise Exception('Columns should not start with numbers! col={}'.format(col))

                    if len(data) > 0:
                        split_by_tags_if_needed_and_push_to_influx(measurement_name, ui_shortname, data,
                                                                   column_names_from_query, tags)
                        logging.info('[%s] %s rows pushed to Influx series "%s"', ui_shortname, len(data), measurement_name)
                    else:
                        logging.debug('[%s] no fresh data found on PgO', ui_shortname)

            except Exception as e:
                logging.error('[%s] ERROR - Could not process %s: %s', ui_shortname, view_name, traceback.format_exc())

            logging.info('[%s] finished processing in %ss', ui_shortname, round(time.time() - host_processing_start_time))


def main():
    parser = ArgumentParser(description='PGObserver InfluxDB Exporter Daemon')
    parser.add_argument('-c', '--config', help='Path to local config file (template file: {})'.format(DEFAULT_CONF_FILE))
    parser.add_argument('--s3-region', help='AWS S3 region for the config file', default=os.environ.get('PGOBS_EXPORTER_CONFIG_S3_REGION'))
    parser.add_argument('--s3-bucket', help='AWS S3 bucket for the config file', default=os.environ.get('PGOBS_EXPORTER_CONFIG_S3_BUCKET'))
    parser.add_argument('--s3-key', help='AWS S3 key for the config file', default=os.environ.get('PGOBS_EXPORTER_CONFIG_S3_KEY'))
    parser.add_argument('--hosts-to-sync', help='only given host_ids (comma separated) will be pushed to Influx')
    parser.add_argument('--drop-db', action='store_true', help='start with a fresh InfluxDB. Needs root login i.e. meant for testing purposes')
    group1 = parser.add_mutually_exclusive_group()
    group1.add_argument('-v', '--verbose', action='store_true', help='more chat')
    group1.add_argument('-d', '--debug', action='store_true', help='even more chat')

    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s %(levelname)s %(threadName)s %(message)s', level=(logging.DEBUG if args.debug
                                                     else (logging.INFO if args.verbose else logging.ERROR)))

    global settings

    if args.config:
        args.config = os.path.expanduser(args.config)
        if os.path.exists(args.config):
            logging.info("Trying to read config file from %s", args.config)
            with open(args.config, 'rb') as fd:
                settings = yaml.load(fd)
    elif args.s3_region and args.s3_bucket and args.s3_key:
        logging.info("Trying to read config file from S3...")
        config_file_as_string = get_s3_key_as_string(args.s3_region, args.s3_bucket, args.s3_key)
        settings = yaml.load(config_file_as_string)

    if not (settings and settings.get('database') and settings.get('influxdb')):
        logging.error('Config info missing - recheck the --config or --s3_config/--s3-region input!')
        parser.print_help()
        exit(1)

    conn_params = {'dbname': settings['database']['name'],
                    'host': settings['database']['host'],
                    'user': settings['database']['frontend_user'],
                    'port': settings['database']['port']}

    logging.info('Setting connection string to: %s', conn_params)
    conn_params['password'] = settings['database']['frontend_password']

    datadb.init_connection_pool(int(settings['max_worker_threads']) + 1, **conn_params)

    idb = influxdb.InfluxDBClient(settings['influxdb']['host'],
                                 settings['influxdb']['port'],
                                 settings['influxdb']['username'],
                                 settings['influxdb']['password'])

    if args.drop_db:
        logging.debug('DBs found from InfluxDB: %s', idb.get_list_database())
        idb_ensure_database(idb, settings['influxdb']['database'], True)
    else:
        idb_ensure_database(idb, settings['influxdb']['database'], False)
    idb.switch_database(settings['influxdb']['database'])

    logging.info('Following views will be synced: %s', settings['data_collection_queries_to_process'])

    hosts_to_sync = []
    if args.hosts_to_sync:
        hosts_to_sync = args.hosts_to_sync.split(',')
        hosts_to_sync = [int(x) for x in hosts_to_sync]
        logging.debug('Syncing only hosts: %s', hosts_to_sync)

    last_queued_time_for_host = collections.defaultdict(dict)
    is_first_loop = True
    workers = []
    active_hosts = []
    active_hosts_refresh_time = 0
    sql_active_hosts = "select host_id as id, replace(lower(host_ui_shortname), '-','') as ui_shortname from monitor_data.hosts " \
                       "where host_enabled and (%s = '{}' or host_id = any(%s)) order by 2"

    while True:

        if time.time() - active_hosts_refresh_time > 180:  # checking for hosts changes every 3 minutes
            try:
                active_hosts, cols = datadb.executeAsDict(sql_active_hosts, (hosts_to_sync, hosts_to_sync))
                active_hosts_refresh_time = time.time()
            except Exception as e:
                if is_first_loop:   # ignore otherwise, db could be down for maintenance
                    raise e
                logging.error('Could not refresh active host info: %s', e)

        if is_first_loop:   # setup
            workers_to_spawn = min(min(len(hosts_to_sync) if hosts_to_sync else settings['max_worker_threads'], settings['max_worker_threads']), len(sql_active_hosts))
            logging.debug('Nr of monitored hosts: %s', len(active_hosts))
            logging.info('Creating %s worker threads...', workers_to_spawn)
            for i in range(0, workers_to_spawn):
                wt = WorkerThread(args)
                wt.start()
                workers.append(wt)

        if queue.qsize() <= len(active_hosts) * 2:

            for ah in active_hosts:

                last_data_pull_time_for_view = last_queued_time_for_host.get(ah['id'], 0)
                if time.time() - last_data_pull_time_for_view < settings.get('min_check_interval_for_host', 30):
                    # logging.debug('Not pulling data as args.check_interval not passed yet [host_id %s]', ah['id'])
                    continue

                logging.debug('Putting %s to queue...', ah['ui_shortname'])
                queue.put({'id': ah['id'], 'ui_shortname': ah['ui_shortname'],
                           'is_first_loop': is_first_loop, 'queued_on': time.time()})
                last_queued_time_for_host[ah['id']] = time.time()

            if is_first_loop:
                is_first_loop = False

        logging.debug('Main thread sleeps...')
        time.sleep(5)


if __name__ == '__main__':
    main()
