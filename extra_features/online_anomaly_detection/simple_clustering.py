import argparse
import json
import logging
import os
from collections import Counter
from collections import defaultdict
import time
from datetime import datetime, timedelta
import glob

import datadb
import email_interface
import timeseries_clusterer


METRIC_DEFINITION_FOLDER = 'metric_definitions'

# NB! All below defined constants will be read from the DB in "continuous" mode

MIN_CLUSTER_ITEMS = 3
MIN_HISTORY_BEFORE_ALERTING_DAYS = 14                # At least so much seconds of data must be processed before testing for alerts
LOOKBACK_HISTORY_DAYS = 30                  # 1 month of last data  # TODO user stored feedback patterns that are kep for 6 months say

CLUSTER_DIFFERENCE_ERROR_PCT = 25           # Threshold for alert condition. if a new cluster's metrics a more different than that from any other cluster signal an alert
BREAKOUT_THRESHOLD_PCT = 15                 # Threshold for cluster MVA breakout. if a new MVA value differs more than that we start to formulate a new cluster (pattern)
ABNORMAL_JUMP_THRESHOLD = 100    # used for dropping single spikes or to alert when value above historical avg
MVA_ITEMS = 3   # MVA will comprise of X last series values
CLUSTER_DIFFERENCE_CALC_WEIGHTS = {"avg": 20, "med": 10, "stddev": 10, "min": 15, "max": 15, "itemcount": 10, "trend": 20}

MAX_PATTERN_LENGTH_MINUTES = 60 * 6       # max pattern duration. if no breakout within that time the running pattern is finalized  # TODO param
WAIT_TIME_BEFORE_CONSECUTIVE_EMAIL_SENDING_PER_HOST_METRIC = 3600


# MIN_CLUSTER_SPAN = 5*60   # in seconds TODO
# http://pandas.pydata.org/pandas-docs/stable/generated/pandas.ewma.html TODO
# http://earthpy.org/pandas-basics.html, maybe can use smth

MIN_SECONDS_BETWEEN_ONE_HOST_METRIC_CHECK = 60
CONFIG_REFRESH_INTERVAL_SECONDS = 2*60

cluster_engines_by_host_metric = defaultdict(dict)    # {host_id: {'metrics': {'cpu': TimeseriesClusterer}}, 'config': config_from_db}
last_check_times_by_host_metric = defaultdict(dict)
last_email_sent_map = defaultdict(dict)
last_config_refresh_time = 0
last_config_as_str = ''
args = None
script_start_datetime = datetime.now()
filter_cache = defaultdict(dict)


def read_series_from_file(file):
    """input file needs to be a CSV with 1st 2 cols: value, epoch"""
    ret = []
    with open(file) as f:
        for line in f.read().splitlines():
            if line:
                x, y, z = line.split(',')
                ret.append({'dt': datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'), 'value': float(y), 'ident': z})
    return ret


def get_active_hosts_and_their_configs(host_ids=None, metrics=None):
    sql = """
        select
          host_id,
          replace(lower(host_ui_shortname), '-', '') as uiname,
          m_metric as metric,
          gdp_params,
          mdp_params,
          hp_params,
          hmp_params
        from
          monitor_data.hosts
          join
          olad.global_default_params on true
          join
          olad.metrics on true
          left join
          olad.monitored_hosts on host_id = mh_host_id and mh_enabled
          left join
          olad.metric_default_params on mdp_metric = m_metric
          left join
          olad.host_params on hp_host_id = host_id
          left join
          olad.host_metric_params on hmp_host_id = host_id and hmp_metric = m_metric
        where
          host_enabled
          and case when %(host_ids)s is null then mh_host_id is not null else host_id = any(%(host_ids)s::int[]) end
          and case when %(metrics)s is null then m_enabled else m_metric = any(%(metrics)s::text[]) end
        order by
          host_id, m_metric
    """
    data = datadb.execute(sql, {'host_ids': host_ids, 'metrics': metrics})

    for d in data:
        for key in ['gdp_params', 'mdp_params', 'hp_params', 'hmp_params']:
            if d.get(key) and type(d[key]) != dict:
                d[key] = json.loads(d[key])
    return data


def log_unknown_pattern_to_db(host_id, metric, ident, change_pct, change_pct_allowed, point_count, dt1, dt2, metrics=None, message=None):
    try:
        sql = """
            INSERT INTO olad.unknown_patterns(
                up_host_id, up_metric, up_ident,
                up_change_pct, up_change_pct_allowed,
                up_points, up_tz1, up_tz2,
                up_metrics, up_message)
            VALUES (%s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s);
        """
        datadb.execute(sql, (host_id, metric, ident, change_pct, change_pct_allowed, point_count, dt1, dt2, json.dumps(metrics), message))
    except:
        logging.exception('failed to store to olad.unknown_patterns!')


def get_unknown_pattern_count_for_host(host_id, metric, interval='1hour'):
    try:
        sql = """
            SELECT
              count(1)
            FROM
              olad.unknown_patterns
            WHERE
              up_host_id = %(host_id)s
              AND up_metric = %(metric)s
              AND up_created_on > now() - %(interval)s::interval
        """
        ret = datadb.execute(sql, {'host_id': host_id, 'metric': metric, 'interval': interval})
        return ret[0]['count']
    except:
        logging.exception('failed to call get_unknown_pattern_count_for_host()!')
        return None


def load_metric_definitions(folder=METRIC_DEFINITION_FOLDER):
    ret = defaultdict(dict)
    files = glob.glob(os.path.join(folder, '*.sql'))
    files.sort()
    for f in files:
        def_info = {}
        try:
            file_content = open(f).read()
            metric_name = os.path.basename(f).replace('.sql', '')
            if metric_name.endswith('_filter'):
                metric_name = metric_name.replace('_filter', '')
                ret[metric_name]['filter'] = file_content
            else:
                ret[metric_name]['query'] = file_content
        except Exception as e:
            logging.error('failed to load definition file %s: %s', f, e)
    return ret


def log_unknown_pattern_and_alert_if_enabled(host_id, uiname, metric, ident, config, diff_pct, allowed_diff_pct,
                                             start_datetime=None, end_datetime=None,
                                             item_count=None, metrics=None, message=None, last_mva=None, historic_avg=None):
    # logging.error('log_unknown_pattern_and_alert_if_enabled(%s)', [host_id, uiname, metric, ident, config, diff_pct, allowed_diff_pct,
    #                                          start_epoch, end_epoch, item_count, metrics, message])

    if args.send_alert_emails and last_email_sent_map[host_id].get(metric, 0) < time.time() - WAIT_TIME_BEFORE_CONSECUTIVE_EMAIL_SENDING_PER_HOST_METRIC:
        if config.get('EMAIL_ADDRESSES'):
            logging.debug('[%s][%s][%s] sending alert email...', host_id, metric, ident)
            email_subject = email_interface.compose_email_subject(uiname, metric, ident, diff_pct, allowed_diff_pct)
            email_body = message if message else email_interface.compose_email_body(uiname, metric, ident, diff_pct, allowed_diff_pct,
                                                                                    additional_message=message, metrics=metrics, from_dt=start_datetime, to_dt=end_datetime,
                                                                                    last_mva=last_mva, historic_avg=historic_avg)
            success = email_interface.try_send_mail(config.get('EMAIL_ADDRESSES'), email_subject, email_body)
            if success:
                last_email_sent_map[host_id][metric] = time.time()
        else:
            logging.warning('no email address configured, cant send alert emails')
    if args.store_alerts:
        logging.debug('[%s][%s] storing unknow pattern to DB...', host_id, metric)
        log_unknown_pattern_to_db(host_id, metric, ident, diff_pct, diff_pct, item_count, start_datetime, end_datetime, metrics, message)


def get_last_tz_for_host_metric(cluster_engines_by_host_metric, host_id, metric, config):
    ret = (datetime.now() - timedelta(days=config['LOOKBACK_HISTORY_DAYS'])).isoformat()
    if args.date_from:
        ret = args.date_from
    if cluster_engines_by_host_metric[host_id].get(metric):
        for ident, cluster_engine in cluster_engines_by_host_metric[host_id].get(metric).items():
            if cluster_engine.prev_point:
                if str(cluster_engine.prev_point['dt'].isoformat()).replace('T', ' ') > str(ret):
                    ret = str((cluster_engine.cur_cluster_items[-1]['dt']).isoformat())
    logging.debug('[%s][%s] get_last_tz_for_host_metric() = %s', host_id, metric, ret)
    return ret


def process_host_metric(sql, host_id, metric, config, uiname='', ident_filter=None):
    """:type tce: TimeseriesClusterer"""
    last_tz = get_last_tz_for_host_metric(cluster_engines_by_host_metric, host_id, metric, config)
    logging.info('[%s][%s] checking for fresh points from PGO. last timestamp: "%s"', max(uiname, host_id), metric, last_tz)

    data = datadb.execute(sql, {'metric': metric, 'host_id': host_id, 'date_from': last_tz, 'filter': ident_filter, 'date_to': None})

    if data:
        for d in data:
            ident = d['ident']
            # cluster_engines_by_host_metric = {host_id: {'metric': {'ident': TimeseriesClusterer}}, 'config': config_from_db}
            if cluster_engines_by_host_metric[host_id].get(metric, {}).get(ident):
                tce = cluster_engines_by_host_metric[host_id][metric][ident]    # TODO fetch metric history if point around time.time()
            else:
                if not cluster_engines_by_host_metric[host_id].get(metric):
                    cluster_engines_by_host_metric[host_id][metric] = {}
                tce = timeseries_clusterer.TimeseriesClusterer(host_id, metric, ident, max(uiname, str(host_id)), config=config)
                logging.info('[%s][%s][%s] TimeseriesClusterer created', host_id, metric, ident)
                tce.abs_peaks = args.abs_peaks
                cluster_engines_by_host_metric[host_id][metric][ident] = tce

            unknown_pattern, alert_message = tce.add_point(d)
            if unknown_pattern and (args.continuous and d['dt'] > script_start_datetime):   # ignore patterns before script start time
                logging.error('[%s][%s][%s] UNKNOWN PATTERN FOUND in range [%s..%s], diff_pct=%s, metrics=%s', uiname, metric, ident,
                              unknown_pattern['items'][0]['dt'], unknown_pattern['items'][-1]['dt'],
                              unknown_pattern['closest_cluster']['diff_pct'], unknown_pattern['metrics'])
                log_unknown_pattern_and_alert_if_enabled(host_id, uiname, metric, ident, config,
                                                         diff_pct=unknown_pattern['closest_cluster']['diff_pct'],
                                                         allowed_diff_pct=config.get('CLUSTER_DIFFERENCE_ERROR_PCT'),
                                                         item_count=len(unknown_pattern['items']),
                                                         start_datetime=unknown_pattern['items'][0]['dt'],
                                                         end_datetime=unknown_pattern['items'][-1]['dt'],
                                                         metrics=unknown_pattern['metrics'])
            elif alert_message:
                pass    # TODO

    return len(data)


def compose_config_from_args(args):
    return {
                "MIN_CLUSTER_ITEMS": args.min_cluster_items,
                "MIN_HISTORY_BEFORE_ALERTING_DAYS": args.min_history_before_alerting_days,
                "LOOKBACK_HISTORY_DAYS": args.lookback_history_days,
                "MAX_PATTERN_LENGTH_MINUTES": args.max_pattern_length_minutes,
                "CLUSTER_DIFFERENCE_ERROR_PCT": args.cluster_difference_error_pct,
                "BREAKOUT_THRESHOLD_PCT": args.breakout_threshold_pct,
                "ABNORMAL_JUMP_THRESHOLD": args.ABNORMAL_JUMP_THRESHOLD,
                "MVA_ITEMS": args.mva_items,
                "CLUSTER_DIFFERENCE_CALC_WEIGHTS": args.cluster_difference_calc_weights,
                "EMAIL_ADDRESSES": args.email_addresses,
                "DROP_SPIKES": args.drop_spikes,
                "NO_ZEROES": args.no_zeroes,
                "NO_FILTERING": args.no_filtering,
            }


def merge_dicts(*args):
    """key from the last dict (if present) will be set"""
    if not args:
        return {}
    for arg in args:
        if arg and type(arg) != dict:
            raise Exception('merge_dicts() args must be dicts! type({})={}'.format(arg, type(arg)))
    ret = args[0].copy()
    for arg in args[1:]:
        if arg:
            for k, v in arg.items():
                ret[k] = v
    return ret


def merge_config_data(host_and_config_data_from_db):
    """host_and_config_data_from_db is a list of following keys: host_id, uiname, m_metric, gdp_params, mdp_params, hp_params, hmp_params"""
    for d in host_and_config_data_from_db:
        d['params'] = merge_dicts(d['gdp_params'], d['mdp_params'], d['hp_params'], d['hmp_params'], compose_config_from_args(args) if args.override_db_config else {})
    return host_and_config_data_from_db


def get_filters_and_refresh_db_if_overdue(sql_filter_update, host_id, metric):
    if filter_cache.get(host_id, {}).get(metric, {}).get('last_update_epoch') > time.time() - 3600:
        return filter_cache.get(host_id).get(metric).get('filters')

    logging.info('[%s][%s] updating filters from DB...', host_id, metric)
    # logging.debug('[%s][%s] sql: %s', host_id, metric, sql_filter_update)
    filters = datadb.execute(sql_filter_update, {'host_id': host_id, 'metric': metric})  # TODO single call with CTEs
    if filters:
        filters = [x['ident'] for x in filters]
    logging.info('[%s][%s] %s filters found', host_id, metric, len(filters) if filters else 0)

    if not filter_cache[host_id].get(metric):
        filter_cache[host_id][metric] = {}
    filter_cache[host_id][metric]['filters'] = filters
    filter_cache[host_id][metric]['last_update_epoch'] = time.time()
    return filters if filters else None


if __name__ == '__main__':
    argp = argparse.ArgumentParser(description='Reads load, avg. sproc runtime (1 sproc) or table scans (1 table) from PGO and clusters data points into pattern before matching them')
    argp.add_argument('--host-id', help='required for --graph mode. ')
    argp.add_argument('-m', '--metric', metavar='cpu_load|sproc_runtime|table_scans')
    argp.add_argument('-f', '--filter', nargs='*', help="relevant for some metrics, e.g. sproc name for sproc_runtime and table full name for seq_scans")
    argp.add_argument('--no-filtering', help="relevant for continuous mode. all ident's are analyzed")
    argp.add_argument('--date-from', help="start date", default=(datetime.now() - timedelta(days=LOOKBACK_HISTORY_DAYS)))
    argp.add_argument('--date-to', help="end date")
    argp.add_argument('-v', '--verbose', action='count', default=0, help='[-v|-vv]')
    modes = argp.add_mutually_exclusive_group(required=True)
    argp.add_argument('--dump', action='store_true', help='just dump read metrics to console')
    modes.add_argument('-g', '--graph', action='store_true', help='read one time input for one host and show a plot')
    modes.add_argument('-c', '--continuous', action='store_true', help='fetch data continuosly from PGO DB (for hosts defined in olad.monitored_hosts) and alert if needed')
    modes.add_argument('--file', help='read series from a file. for --graph mode')

    # clustering parameters
    argp.add_argument('--min-history-before-alerting-days', default=MIN_HISTORY_BEFORE_ALERTING_DAYS, help='at least so much data must be processed before testing for alerts', metavar=MIN_HISTORY_BEFORE_ALERTING_DAYS, type=int)
    argp.add_argument('--lookback-history-days', default=LOOKBACK_HISTORY_DAYS, help='how long to keep patterns in memory', metavar=LOOKBACK_HISTORY_DAYS, type=int)
    argp.add_argument('--max-pattern-length-minutes', default=MAX_PATTERN_LENGTH_MINUTES, help='max pattern length if no breakout occurs', metavar=MAX_PATTERN_LENGTH_MINUTES, type=int)
    argp.add_argument('--cluster-difference-error-pct', default=CLUSTER_DIFFERENCE_ERROR_PCT, help="Threshold for alert condition. if a new cluster's metrics a more different than that from any other cluster signal an alert",
                      metavar=CLUSTER_DIFFERENCE_ERROR_PCT, type=int)
    argp.add_argument('--breakout-threshold-pct', default=BREAKOUT_THRESHOLD_PCT, help="Threshold for cluster MVA breakout. if a new MVA value differs more than that we start to formulate a new cluster (pattern)",
                      metavar=BREAKOUT_THRESHOLD_PCT, type=int)
    argp.add_argument('--abnormal-jump-threshold', default=100, help="if one breakout follows other we consider it a spike and drop if change bigger than that",
                      metavar=100, type=int)
    argp.add_argument('--min-cluster-items', default=MIN_CLUSTER_ITEMS, help="Minum points needed to form a pattern", metavar=MIN_CLUSTER_ITEMS, type=int)
    argp.add_argument('--mva-items', default=MVA_ITEMS, help="MVA (moving average) will comprise of X last series values", metavar=MVA_ITEMS, type=int)
    argp.add_argument('--cluster-difference-calc-weights', default=CLUSTER_DIFFERENCE_CALC_WEIGHTS, metavar=CLUSTER_DIFFERENCE_CALC_WEIGHTS)
    argp.add_argument('--override-db-config', action='store_true', help='when set will override DB config in --continouous mode')

    # alerting flags
    argp.add_argument('--abs-peaks', action='store_true', help='only alert if value is > than any known cluster avg')
    argp.add_argument('--drop-spikes', action='store_true', help='drop skyrocketing single points')
    argp.add_argument('--no-zeroes', action='store_true', help='if flag set and MVA == 0 then alert')
    argp.add_argument('--send-alert-emails', action='store_true', help='send alert emails')
    argp.add_argument('--email-addresses', help='csv list of emails. effective only when --send-alert-emails set', default='')
    argp.add_argument('--store-alerts', action='store_true', help='store "unknown pattern found" events to DB (olad.unknown_patterns)')

    # PGO datastore connect details
    argp.add_argument('-H', '--host', default=os.getenv('PGOBS_HOST'), help='DB host')
    argp.add_argument('-p', '--port', default=os.getenv('PGOBS_PORT', 5432), help='DB port')
    argp.add_argument('-d', '--database', default=os.getenv('PGOBS_DATABASE'), help='DB name')
    argp.add_argument('-U', '--user', default=os.getenv('PGOBS_USER', os.getenv('USER')), help='DB user')
    argp.add_argument('--password', default=os.getenv('PGOBS_PASSWORD', ''), help='DB password')

    args = argp.parse_args()

    log_levels = {0: logging.ERROR, 1: logging.WARNING, 2: logging.INFO, 3: logging.DEBUG}
    logging.basicConfig(level=log_levels[args.verbose], format='%(asctime)s %(levelname)s %(message)s')
    args_p = dict(vars(args))
    args_p['password'] = 'xxx'
    logging.info('args: %s', args_p)

    if args.file:
        if not os.path.exists(args.file):
            print 'input file not found: {}'.format(args.file)
            exit()
        args.graph = True

    if args.host:
        logging.info('checking DB connection...')
        datadb.init_connection_pool(max_conns=1, host=args.host, port=args.port, dbname=args.database, user=args.user, password=args.password)
        if not datadb.is_data_store_connection_ok():
            logging.error('could not connect to PGO DB')
            exit()
        logging.info('DB connection OK!')

    if type(args.cluster_difference_calc_weights) != dict:
        args.cluster_difference_calc_weights = json.loads(args.cluster_difference_calc_weights)

    metric_definition_queries = load_metric_definitions()
    if args.metric and args.metric not in metric_definition_queries:
        print 'sql definition for "{}" not found. (found metrics: {})'.format(args.metric, metric_definition_queries.keys())
        exit()

    if args.dump or args.graph or args.file:

        if args.file:
            data = read_series_from_file(args.file)
            host_id = 1
            uiname = 'host'
        else:
            logging.warning('reading metric "%s" for host %s from PGO [%s..%s]', args.metric, args.host_id, args.date_from, args.date_to)
            logging.debug('query: %s', metric_definition_queries[args.metric])
            data = datadb.execute(metric_definition_queries[args.metric]['query'],
                                  {'host_id': args.host_id, 'date_from': args.date_from, 'date_to': args.date_to, 'filter': args.filter})
            host_config_by_metric = get_active_hosts_and_their_configs([args.host_id])   # just for getting the uiname
            uiname = host_config_by_metric[0]['uiname']
            host_id = args.host_id
        if not data:
            logging.error('0 points found. exiting')
            exit()

        if args.dump:
            for d in data:
                print d['dt'], '\t', d['value'], '\t', d['ident']
            exit()

        if args.graph:
            import matplotlib.pyplot as plt

            config = compose_config_from_args(args)
            ident = data[0]['ident']
            tc = timeseries_clusterer.TimeseriesClusterer(max(args.host_id, host_id), args.metric, config=config, uiname=uiname, ident=ident, graph_mode=True)

            for d in data[:-1]:
                if d['ident'] != ident:
                    raise Exception('Only 1 ident allowed in --graph mode! Ident1={}, Ident2={}. Use --filter flag'.format(ident, d['ident']))

                unknown_pattern, alert_message = tc.add_point(d)
                if unknown_pattern: # and (args.continuous and d['dt'] > time.time() - IGNORE_ALERTS_OLDER_THAN_SECONDS):
                    log_unknown_pattern_and_alert_if_enabled(args.host_id, uiname, args.metric, ident, config,
                                                             diff_pct=unknown_pattern['closest_cluster']['diff_pct'],
                                                             allowed_diff_pct=args.cluster_difference_error_pct,
                                                             item_count=len(unknown_pattern['items']),
                                                             start_datetime=unknown_pattern['items'][0]['dt'],
                                                             end_datetime=unknown_pattern['items'][-1]['dt'],
                                                             metrics=unknown_pattern['metrics'],
                                                             message=alert_message)

            logging.warning('clusters found: %s', len(tc.clusters))
            cluster_sizes = [len(x['items']) for x in tc.clusters]
            logging.warning('avg. cluster size: %s',  sum(cluster_sizes) / len(tc.clusters))
            logging.warning('points dropped: %s (of total: %s)',  tc.points_dropped, len(data))
            logging.warning('pattern comparisons performed: %s',  tc.pattern_comparisons_performed)
            logging.warning('trends distribution: %s', Counter([c['metrics']['trend'] for c in tc.clusters]))
            for i, cl in enumerate(tc.clusters):
                logging.debug('cluster %s: items=%s, metrics=%s, tz_min=%s, tz_max=%s',
                             i, len(cl['items']), cl['metrics'], cl['items'][0]['dt'], cl['items'][-1]['dt'])

                # plot - http://matplotlib.org/api/pyplot_api.html#matplotlib.pyplot.plot
                plt.plot([c['dt'] for c in cl['items']], [c['value'] for c in cl['items']])

            logging.warning('%s unknown patterns found', len(tc.clusters_with_unknown_patterns))
            for i, unk in enumerate(tc.clusters_with_unknown_patterns, 1):
                logging.warning('unknown pattern %s [%s]: %s..%s', i, ident, unk['dt1'], unk['dt2'])
                logging.warning('metrics: %s', unk['metrics'])
                logging.warning('closest known pattern: %s', unk['closest_cluster'])
                plt.plot([c['dt'] for c in unk['items']], [c['value'] for c in unk['items']], '*')
            plt.show()
    elif args.continuous:
        try:
            host_and_config_data_from_db = []
            while True:
                if last_config_refresh_time < time.time() - CONFIG_REFRESH_INTERVAL_SECONDS:
                    logging.info('[main] getting list of hosts to be monitored from PGO...')
                    host_and_config_data_from_db = get_active_hosts_and_their_configs(args.host_id.split(',') if args.host_id else None,
                                                                                      args.metric.split(',') if args.metric else None)
                    host_and_config_data_from_db = merge_config_data(host_and_config_data_from_db)
                    logging.info('[main] %s active host/metric configs found from DB', len(host_and_config_data_from_db))
                    config_str_list = []
                    [config_str_list.append('{}:{}:{}'.format(x['host_id'], x['metric'], x['params'])) for x in host_and_config_data_from_db]
                    config_str = ''.join(config_str_list)
                    if last_config_refresh_time == 0:
                        logging.warning('[main] config for all hosts: %s', config_str_list)
                    elif last_config_as_str != config_str:
                        logging.warning('[main] config change detected. config for all hosts: %s', config_str_list)
                    last_config_as_str = config_str
                    last_config_refresh_time = time.time()

                if len(host_and_config_data_from_db) == 0:
                    logging.warning('[main] no active hosts found. sleeping 30s before re-check')
                    time.sleep(60)
                else:
                    for host_data in host_and_config_data_from_db:
                        host_id = host_data['host_id']
                        uiname = host_data['uiname']
                        params = host_data['params']
                        metric = host_data['metric']

                        logging.debug('[main] processing [%s][%s] (host_id=%s, params=%s)', uiname, metric, host_id, params)

                        if last_check_times_by_host_metric[host_id].get(metric, 0) > time.time() - MIN_SECONDS_BETWEEN_ONE_HOST_METRIC_CHECK:
                            continue

                        ident_filter = None
                        if args.filter:
                            ident_filter = args.filter
                        else:
                            if not params.get('NO_FILTERING') and metric_definition_queries[metric].get('filter'):
                                ident_filter = get_filters_and_refresh_db_if_overdue(metric_definition_queries[metric]['filter'], host_id, metric)

                        logging.info('[%s][%s] checking for fresh data from PGO DB', uiname, metric)

                        points_added = process_host_metric(metric_definition_queries[metric]['query'], host_id, metric=metric, config=params, uiname=uiname, ident_filter=ident_filter)

                        logging.info('[%s][%s] %s points added', uiname, metric, points_added)
                        last_check_times_by_host_metric[host_id][metric] = time.time()
                        time.sleep(0.1)
                time.sleep(0.1)

        except KeyboardInterrupt:
            logging.info('KeyboardInterrupt...')
