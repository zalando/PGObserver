import importlib
from argparse import ArgumentParser
import json
import logging
import os
import time
import collections

import psycopg2

import pgobserver_gatherer.globalconfig as globalconfig
import pgobserver_gatherer.datadb as datadb
import pgobserver_gatherer.datastore_adapter as datastore_adapter
import pgobserver_gatherer.gatherer_base as gatherer_base
import pgobserver_gatherer.gatherer_load as gatherer_load
import pgobserver_gatherer.gatherer_schemas as gatherer_schemas
import pgobserver_gatherer.gatherer_sproc as gatherer_sproc
import pgobserver_gatherer.gatherer_database as gatherer_database
import pgobserver_gatherer.gatherer_statements as gatherer_statements
import pgobserver_gatherer.gatherer_bgwriter as gatherer_bgwriter
import pgobserver_gatherer.gatherer_locks as gatherer_locks
import pgobserver_gatherer.gatherer_index as gatherer_index
import pgobserver_gatherer.gatherer_table as gatherer_table
import pgobserver_gatherer.gatherer_table_io as gatherer_table_io
import pgobserver_gatherer.gatherer_connection as gatherer_connection
import pgobserver_gatherer.gatherer_kpi as gatherer_kpi
from pgobserver_gatherer.output_handlers.handler_base import HandlerBase
from pgobserver_gatherer.globalconfig import Datasets
from pgobserver_gatherer.globalconfig import output_handlers

SQL_ACTIVE_HOSTS = '''select * from monitor_data.hosts where host_enabled and host_gather_group = %s'''
SQL_HOST_UNDER_TEST_BY_ID = '''select * from monitor_data.hosts where host_id = %s'''
HOST_CHANGES_CHECK_INTERVAL = 300
DEFAULT_CONF_FILE = './pgobserver_gatherer.yaml'

gatherers_by_host_id = collections.defaultdict(dict)    #   {host_id: {'Load': prc, ...}, ...}
gatherer_intervals_snapshot = collections.defaultdict(dict) # {host_id: {'Load': x}}


def launch_suitable_worker_and_return_process(key, host_data, settings):
    logging.debug('[main][%s] looking for suitable worker for key = %s', host_data['host_ui_shortname'], key)
    p = None
    if key == 'loadGatherInterval':
        p = gatherer_load.LoadGatherer(host_data, settings)
    elif key == 'sprocGatherInterval':
        p = gatherer_sproc.SprocGatherer(host_data, settings)
    elif key == 'statDatabaseGatherInterval':
        p = gatherer_database.DatabaseGatherer(host_data, settings)
    elif key == 'statStatementsGatherInterval':
        p = gatherer_statements.StatStatementsGatherer(host_data, settings)
    elif key == 'statBgwriterGatherInterval':
        p = gatherer_bgwriter.BgwriterGatherer(host_data, settings)
    elif key == 'blockingStatsGatherInterval':
        p = gatherer_locks.BlockingLocksGatherer(host_data, settings)
    elif key == 'indexStatsGatherInterval':
        p = gatherer_index.IndexGatherer(host_data, settings)
    elif key == 'schemaStatsGatherInterval':
        p = gatherer_schemas.SchemaStatsGatherer(host_data, settings)
    elif key == 'tableStatsGatherInterval':
        p = gatherer_table.TableStatsGatherer(host_data, settings)
    elif key == 'tableIoGatherInterval':
        p = gatherer_table_io.TableIOStatsGatherer(host_data, settings)
    elif key == 'statConnectionGatherInterval':
        p = gatherer_connection.ConnectionStatsGatherer(host_data, settings)
    elif key == 'KPIGatherInterval':
        p = gatherer_kpi.KPIGatherer(host_data, settings)
    if p:
        p.daemon = True
        p.start()
    return p


def get_active_and_nonactive_gatherer_names_from_host_settings(host_id, host_settings):
    active = []
    try:
        settings = json.loads(host_settings)
        for k, v in settings.items():
            if k in globalconfig.ALL_GATHERER_INTERVAL_KEYS and v > 0:
                active.append(k)
            elif k not in globalconfig.ADDITIONAL_GATHERER_SETTING_KEYS:
                logging.warning('[main] found unknown host_setting key %s for host_id %s', k, host_id)
    except Exception as e:
        logging.error('[main] error processing host_settings for host_id %s: %s', host_id, host_settings)

    return active


def load_output_handlers(console_only=False, filters=None):
    if console_only:
        output_handlers['console'] = {'type': 'console'}
    else:
        for name, params in globalconfig.config.get('output_plugins').items():
            if params.get('enabled'):
                output_handlers[name] = params

    if not output_handlers:
        logging.warning('no output handlers found! only raw metrics will be stored if store_raw_metrics=true')
        return

    i = 0
    for handler_name, config in output_handlers.items():
        logging.info('loading output plugin: "%s" with config - %s', handler_name, config)
        try:
            handler_type = config['type']
            module = importlib.import_module('pgobserver_gatherer.output_handlers.{}.{}_handler'.format(handler_type, handler_type))
            handler_class = getattr(module, 'Handler')
            handler = handler_class(config, filters)
            globalconfig.output_handlers[handler_name] = handler
            handler.start()
            logging.debug('handler %s started: %s', handler_name, handler)
            i += 1
        except Exception as e:
            logging.exception('could not find handler "%s" to load', handler_type)
    logging.info('%s output handlers activated', i)


def main():
    parser = ArgumentParser(description='pgobserver3 gatherer daemon')
    group_operate_mode = parser.add_mutually_exclusive_group(required=True)
    group_operate_mode.add_argument('-c', '--config', help='Path to yaml config file with datastore connect details')
    group_operate_mode.add_argument('--init-config', action='store_true', help='Create a sample yaml config file in working dir')
    group_operate_mode.add_argument('-o', '--console', action='store_true', help='Monitor a single host and print to console')

    parser.add_argument('-v', '--verbose', help='chat level. none(default)|-v|-vv [$VERBOSE=[0|1|2]]', action='count', default=(os.getenv('VERBOSE') or 0))

    # console mode. live tracking of 1 metric on a specified host. expects passwordless connection (.pgpass or trust)
    group_console = parser.add_argument_group('Console mode params')
    group_console.add_argument('-H', '--host', help='Monitored DB host')
    group_console.add_argument('-p', '--port', help='Monitored DB port', default=5432, type=int)
    group_console.add_argument('-d', '--dbname', help='Monitored DB name')
    group_console.add_argument('-U', '--username', help='Monitored DB username', default=os.getenv('USER'))
    group_console.add_argument('--password', help='Monitored DB password', default=os.getenv('PGO_PASS'))
    group_console.add_argument('-g', '--gatherer', default='kpi',
                               help='gatherer interval name. one of: [ {} ]'.format(' | '.join(sorted(list(globalconfig.SUPPORTED_DATASETS.keys())))),)
    group_console.add_argument('-i', '--interval', help='interval in seconds between metric gatherings. default 5', default=5, type=int)
    group_console.add_argument('-n', '--no-deltas', action='store_true', help='don''t calculate diffs for predefined columns')
    group_console.add_argument('-f', '--filters', nargs='*',
                               help='filter(s) for "key columns" (if defined for a metric) to enable monitoring of a specific table')   # TODO add wildcards
    group_console.add_argument('-r', '--human-readable', action='store_true', help='Human readable units for big numbers/sizes')    # TODO

    # params specifying the datastore. if all present then config file is not used.
    group_datastore_connect_data = parser.add_argument_group('Connection string to metrics datastore Postgres DB')
    group_datastore_connect_data.add_argument('--ds-host', help='datastore hostname [$DS_HOST]', default=os.getenv('DS_HOST'))
    group_datastore_connect_data.add_argument('--ds-port', help='datastore port [$DS_PORT]', default=(os.getenv('DS_PORT') or 5432))
    group_datastore_connect_data.add_argument('--ds-dbname', help='datastore dbname [$DS_DBNAME]', default=os.getenv('DS_DBNAME'))
    group_datastore_connect_data.add_argument('--ds-pool-size', help='datastore password [$DS_POOL_SIZE]', default=(os.getenv('DS_POOL_SIZE') or 10))
    group_datastore_connect_data.add_argument('--ds-user', help='datastore username [$DS_USER]', default=os.getenv('DS_USER'))
    group_datastore_connect_data.add_argument('--ds-pass', help='datastore password [$DS_PASS]', default=os.getenv('DS_PASS'))
    group_datastore_connect_data.add_argument('--ds-gather-group', help='gatherer group [$DS_GATHER_GROUP]', default=(os.getenv('DS_GATHER_GROUP') or 'gatherer1'))

    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s %(levelname)s %(process)d %(message)s',
                        level=(logging.DEBUG if int(args.verbose) >= 2 else (logging.INFO if int(args.verbose) == 1 else logging.ERROR)))
    logging.debug(args)

    if args.init_config:
        filename = 'pgobserver_gatherer_sample.yaml'
        globalconfig.write_a_sample_config_file(filename)
        print("{} written. exiting...".format(filename))
        exit(0)
    elif args.config:
        if not os.path.exists(args.config):
            logging.error('Config file "%s" not found! Specify an existing file with -c/--config', args.config)
            logging.error('Tip: use --init-config for creating a sample config in working dir')
            return
        logging.info('reading datastore configuration from file %s ...', args.config)
        globalconfig.read_config_from_yaml(args.config)
    elif args.ds_host and args.ds_dbname and args.ds_user and args.ds_pass and args.ds_gather_group:
        logging.info('initializing datastore config from command line params...')
        globalconfig.set_config_values('database', {'host': args.ds_host, 'name': args.ds_dbname, 'port': args.ds_port,
                                                    'pool_size': args.ds_pool_size, 'backend_user': args.ds_user,
                                                    'backend_password': args.ds_pass, 'gather_group': args.ds_gather_group})
    elif args.console:
        if not (args.host and args.dbname and args.username):
            print('--host, --dbname, --username [ --gatherer | --interval] are needed for console mode!')
            exit(1)
        if args.gatherer not in globalconfig.SUPPORTED_DATASETS:
            print('Invalid value for -g/--gatherer! Valid values:\n-----------')
            print('\n'.join(sorted(globalconfig.SUPPORTED_DATASETS.keys())))
            exit(1)
        logging.info('Starting in console mode!')
        globalconfig.set_config_values('database', {'store_raw_metrics': False})
        globalconfig.set_config_values('features', {'calculate_deltas': False if args.no_deltas else True,
                                                    'simple_deltas': True})
    else:
        parser.print_help()
        exit(1)

    load_output_handlers(console_only=args.console, filters=args.filters)

    logging.debug('globalconfig.config: %s', globalconfig.config)

    if not args.console:
        datadb.setConnectionString(host=globalconfig.config['database']['host'], port=globalconfig.config['database']['port'],
                                   dbname=globalconfig.config['database']['name'], username=globalconfig.config['database']['backend_user'],
                                   password=globalconfig.config['database']['backend_password'])
        if not datadb.isDataStoreConnectionOK():
            logging.fatal('could not connect do datastore. please re-check the configuration. exiting...')
            exit(1)

    is_first_run = True

    while True:
        host_data = []
        new_hosts = set()
        gone_hosts = set()
        old_hosts = set()

        try:
            logging.debug('[main] checking for new/removed hosts...')

            if args.console:
                test_settings = {globalconfig.SUPPORTED_DATASETS[args.gatherer][0]: args.interval / 60.0, 'testMode': 1}
                host_data = [{'host_name': args.host, 'host_port': args.port, 'host_user': args.username, 'host_db': args.dbname,
                              'host_settings': json.dumps(test_settings), 'host_password': args.password,
                              'host_ui_shortname': args.host, 'host_id': 0}]
            else:   # read hosts from metrics datastore
                host_data = datadb.execute(SQL_ACTIVE_HOSTS, (globalconfig.config['database']['gather_group'],))

            host_data_by_id = {}
            for d in host_data:
                host_data_by_id[d['host_id']] = d

            if len(host_data) == 0:
                logging.warning('no active hosts found')
                if len(gatherers_by_host_id) > 0:
                    logging.warning('all workers will be terminated...')    # should build in some safety check ?
                    gone_hosts = list(gatherers_by_host_id.keys())
            else:
                new_hosts = set(host_data_by_id.keys()) - set(gatherers_by_host_id.keys())
                gone_hosts = set(gatherers_by_host_id.keys()) - set(host_data_by_id.keys())
                old_hosts = set(host_data_by_id.keys()) - new_hosts - gone_hosts

            if is_first_run:
                logging.info('1st run. checking all hosts for connectivity...')
                error_count = datastore_adapter.check_all_hosts_for_connectivity(host_data)
                is_first_run = False
                if error_count:
                    logging.error('*** WARNING failed to connect to %s hosts. check connect data! ***', error_count)
                else:
                    logging.info('[main] connectivity check OK for all %s hosts!', len(host_data))

            if new_hosts:
                logging.info('[main] processing new hosts (%s found)', len(new_hosts))
                for host_id in new_hosts:
                    host_ui_shortname = host_data_by_id[host_id]['host_ui_shortname'].lower().replace('-', '')
                    host_settings = host_data_by_id[host_id]['host_settings']
                    logging.info('[main] processing host: %s (id = %s)', host_ui_shortname, host_id)

                    active_gatherers = get_active_and_nonactive_gatherer_names_from_host_settings(host_id, host_settings)
                    for gatherer_name in active_gatherers:
                        settings = json.loads(host_settings)
                        logging.info('[main] loading worker "%s"."%s" with settings: %s', host_ui_shortname,
                                      gatherer_name,  settings)
                        p = launch_suitable_worker_and_return_process(gatherer_name, host_data_by_id[host_id], settings)
                        if p:
                            gatherers_by_host_id[host_id][gatherer_name] = p
                            logging.info('[main] "%s" gatherer processes started for host %s (host_id = %s)',
                                        p.gatherer_name, host_ui_shortname, host_id)
                            gatherer_intervals_snapshot[host_id][gatherer_name] = p.interval_seconds
                        else:
                            logging.error('could not start worker process for key %s (host %s)', gatherer_name, host_id)
                        time.sleep(0)

            if gone_hosts:
                for host_id in gone_hosts:
                    logging.info('[main] terminating workers for host_id %s...', host_id)
                    while len(gatherers_by_host_id[host_id]) > 0:
                        gatherer, p = gatherers_by_host_id[host_id].popitem()
                        if p.is_alive():
                            p.terminate()
                            p.join()    # zombie process danger?
                            logging.info('[main] terminated %s', gatherer)
                        else:
                            logging.warning('[main] could not terminate %s, already not alive', gatherer)
                    gatherers_by_host_id.pop(host_id)
                    gatherer_intervals_snapshot.pop(host_id)
                logging.info('[main] all workers successfully terminated')

            # detect changes in gatherers added/removed + interval changes
            for host_id in old_hosts:
                logging.debug('[main] checking single gatherer changes for %s', host_id)

                active = get_active_and_nonactive_gatherer_names_from_host_settings(host_id, host_data_by_id[host_id]['host_settings'])
                gatherers_added = set(active) - set(gatherer_intervals_snapshot[host_id].keys())
                gatherers_removed = set(gatherer_intervals_snapshot[host_id].keys()) - set(active)
                gatherers_old = set(active) - gatherers_added - gatherers_removed

                for gatherer in gatherers_added:
                    logging.debug('[main] adding new gatherer for %s: %s', host_id, gatherers_added)
                    p = launch_suitable_worker_and_return_process(gatherer_name, host_data_by_id[host_id], settings)
                    if p:
                        gatherers_by_host_id[host_id][gatherer] = p
                        gatherer_intervals_snapshot[host_id][gatherer] = p.interval_seconds

                for gatherer in gatherers_removed:
                    logging.debug('[main] removed gatherers for %s: %s', host_id, gatherers_removed)
                    p = gatherers_by_host_id[host_id][gatherer]
                    if p.is_alive():
                        p.terminate()
                        p.join()    # zombie process danger?
                        logging.info('[main] terminated %s for host %s', gatherer, host_id)
                    gatherers_by_host_id[host_id].pop(gatherer)
                    gatherer_intervals_snapshot[host_id].pop(gatherer)

                for gatherer in gatherers_old:
                    if gatherers_by_host_id[host_id][gatherer].interval_seconds != gatherer_intervals_snapshot[host_id][gatherer]:
                        host_settings = json.loads(host_data_by_id[host_id]['host_settings'])
                        # update worker processes "gather_interval" variable directly. should be safe in our case
                        gatherers_by_host_id[host_id][gatherer_name].interval_seconds = host_settings[gatherer]
                        gatherer_intervals_snapshot[host_id][gatherer] = host_settings[gatherer]

            logging.debug('[main] sleeping %s s...', HOST_CHANGES_CHECK_INTERVAL)
            time.sleep(HOST_CHANGES_CHECK_INTERVAL)

        except psycopg2.OperationalError as e:
            logging.error('[main] failed to query metrics database, sleeping 10s...')
            logging.error(e)
            time.sleep(10)

        except KeyboardInterrupt:
            break

        except Exception as e:
            logging.error('[main] main loop failed: %s', e)
            raise e


if __name__ == '__main__':
    main()


