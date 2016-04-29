import logging
from datetime import timedelta, datetime

import math_helpers


class TimeseriesClusterer:

    def __init__(self, host_id, metric, ident, uiname, config=None, graph_mode=False):
        self.clusters = []      # [{'items': [], 'metrics': {'mva': x, 'min': min, 'max': max}}]
        self.clusters_with_unknown_patterns = []
        self.cur_cluster_items = []
        self.last_values = []
        self.prev_point = None
        self.prev_mva = None
        self.prev_metrics = None
        self.points_processed = 0
        self.points_dropped = 0
        self.pattern_comparisons_performed = 0
        self.host_id = host_id
        self.uiname = max(uiname, str(host_id))
        self.metric = metric
        self.ident = ident
        self.config = config if config else {}
        self.graph_mode = graph_mode    # in non-graph mode we can only store 1st and last data points for clusters to save memory
        self.average_calculator = math_helpers.TimeSeriesAverageCalculator(window_age_seconds=int(config.get('LOOKBACK_HISTORY_DAYS', '14')) * 24 * 3600, no_zeroes=True)

    def enough_data_for_alerting(self, value_dt):
        return len(self.clusters) > 0 and self.clusters[0]['items'][0]['dt'] <= (value_dt - timedelta(days=self.config['MIN_HISTORY_BEFORE_ALERTING_DAYS']))

    def is_valid_point(self, point):
        if self.prev_point and point['dt'] <= (self.prev_point['dt'] + timedelta(seconds=1)):   # more than 1 points per second is a gatherer bug
            return False
        return True

    def add_point(self, point_in):  # point must be a dict of {'value': numeric, 'dt': datetime, 'ident': 'xyz'}
        """returns unknown_pattern dict if a breakout occurs and captured pattern is not familiar, None otherwise"""
        logging.debug('[%s][%s][%s] adding point: %s', self.uiname, self.metric, self.ident, point_in)
        point = point_in.copy()

        if not self.is_valid_point(point):
            logging.debug('[%s][%s][%s] skipping invalid point as timestamp behind last point or within the same second as last. current: %s, prev: %s',
                         self.uiname, self.metric, self.ident, point['dt'].isoformat(), self.prev_point['dt'].isoformat())
            return None, None

        self.points_processed += 1
        self.last_values.append(point['value'])
        if len(self.last_values) > self.config['MVA_ITEMS'] + 1:
            self.last_values.pop(0)
        hist_window_avg_value = self.average_calculator.add_point(point['value'], point['dt'])  # should unite somehow with self.last_values  
        new_cluster_found = False
        similar_pattern_found = False
        alert_message = False

        if self.points_processed < self.config['MVA_ITEMS']:
            logging.debug('[%s][%s][%s] not enough data points yet...(%s needed)', self.uiname, self.metric, self.ident, self.config['MVA_ITEMS'])
            self.prev_point = point
            self.cur_cluster_items.append(point)
            logging.debug('[%s][%s][%s] added point to running pattern. new item count: %s', self.uiname, self.metric,
                          self.ident, len(self.cur_cluster_items))
            return None, None
        cur_mva = math_helpers.calculate_moving_average_for_last_x_items(self.last_values, self.config['MVA_ITEMS'])

        if self.points_processed >= self.config['MVA_ITEMS'] + 1:
            if self.prev_mva == 0 and cur_mva > 0:
                pct_diff_to_last_mva = 100
            elif self.prev_mva == 0 and cur_mva == 0:
                pct_diff_to_last_mva = 0
            else:
                pct_diff_to_last_mva = (abs(cur_mva - self.prev_mva) / float(self.prev_mva)) * 100
            logging.debug("[%s][%s][%s] prev_mva: %s, cur_mva: %s, diff_pct: %s", self.uiname, self.metric, self.ident, self.prev_mva, cur_mva, pct_diff_to_last_mva)

            # value jump
            if pct_diff_to_last_mva > self.config['BREAKOUT_THRESHOLD_PCT'] or \
                    (len(self.cur_cluster_items) > self.config['MIN_CLUSTER_ITEMS'] and
                    ((self.cur_cluster_items[-1]['dt'] - self.cur_cluster_items[0]['dt']).total_seconds() > self.config['MAX_PATTERN_LENGTH_MINUTES'] * 60)):
                point['breakout'] = True

                if len(self.cur_cluster_items) >= self.config['MIN_CLUSTER_ITEMS']:
                    new_cluster_found = True

                    if (self.cur_cluster_items[-1]['dt'] - self.cur_cluster_items[0]['dt']).total_seconds() > self.config['MAX_PATTERN_LENGTH_MINUTES'] * 60:
                        logging.info('[%s][%s][%s] MAX_PATTERN_LENGTH_MINUTES exceeded. [%s] [diff = %s%% (%s%%)]. storing pattern with %s items',
                                        self.uiname, self.metric, self.ident, point['dt'], round(pct_diff_to_last_mva, 2), self.config['BREAKOUT_THRESHOLD_PCT'], len(self.cur_cluster_items))
                    else:
                        logging.debug('[%s][%s][%s] BREAKOUT found [%s] [diff = %s%% (%s%%)]. storing pattern with %s items',
                                        self.uiname, self.metric, self.ident, point['dt'], round(pct_diff_to_last_mva, 2), self.config['BREAKOUT_THRESHOLD_PCT'], len(self.cur_cluster_items))

                    metrics = math_helpers.calculate_cluster_metrics([x['value'] for x in self.cur_cluster_items])

                    # check how pattern relates to historical ones
                    if self.enough_data_for_alerting(point['dt']):    # only when we have at least one "season" of points
                        self.pattern_comparisons_performed += 1
                        diff_pct_for_clusters = []
                        for cluster in self.clusters:
                            diff_pct = math_helpers.calculate_cluster_distance_from_metrics(metrics, cluster['metrics'], self.config['CLUSTER_DIFFERENCE_CALC_WEIGHTS'])
                            if diff_pct <= self.config['CLUSTER_DIFFERENCE_ERROR_PCT']:
                                similar_pattern_found = True
                                logging.debug('[%s][%s][%s] similar_pattern_found: %s < %s', self.uiname, self.metric, self.ident, diff_pct, self.config['CLUSTER_DIFFERENCE_ERROR_PCT'])
                                break
                            else:
                                diff_pct_for_clusters.append((diff_pct, cluster['items'][0]['dt'],
                                                              cluster['items'][-1]['dt'], cluster['metrics']))

                        if not similar_pattern_found:
                            diff_pct_for_clusters.sort(key=lambda x: x[0])

                            if self.config.get('ABS_PEAKS'):
                                max_avg = max([x['metrics']['avg'] for x in self.clusters])
                                if metrics['avg'] < max_avg:
                                    logging.info('[%s][%s][%s] skipping alerting on pattern as max. avg not exceeded: %s < %s', self.uiname, self.metric, self.ident, metrics['avg'], max_avg)
                                    similar_pattern_found = True

                        if not similar_pattern_found:
                            alert_message = 'no similar pattern found'
                            self.clusters_with_unknown_patterns.append({'dt1': self.cur_cluster_items[0]['dt'],
                                                                        'dt2': self.cur_cluster_items[-1]['dt'],
                                                                        'closest_cluster': {
                                                                                'diff_pct': diff_pct_for_clusters[0][0],
                                                                                'dt1': str(diff_pct_for_clusters[0][1]),
                                                                                'dt2': str(diff_pct_for_clusters[0][2]),
                                                                                'metrics': diff_pct_for_clusters[0][3]},
                                                                        'items': self.cur_cluster_items,
                                                                        'metrics': metrics
                                                                        })
                            # TODO flags ignoring up/down/flat
                    else:
                        logging.debug('[%s][%s][%s] cannot compare metrics as not enough data processed', self.uiname, self.metric, self.ident)

                    if self.graph_mode:
                        self.clusters.append({'items': self.cur_cluster_items, 'metrics': metrics})
                    else:   # save some RAM, store only first/last item as not drawing graphs
                        self.clusters.append({'items': [self.cur_cluster_items[0], self.cur_cluster_items[-1]], 'metrics': metrics})

                    self.cur_cluster_items = []

                else:   # len(self.cur_cluster_items) < MIN_CLUSTER_ITEMS i.e. there was a breakout recently
                    if self.config.get('DROP_SPIKES') and self.prev_point.get('breakout'):         # drop a single "spike" upwards
                        if pct_diff_to_last_mva > self.config['ABNORMAL_JUMP_THRESHOLD'] and cur_mva < self.prev_mva:    # should consider also negative jumps?
                            #  log an error still if "abs_peaks" flag set and value > "max avg known cluster value"
                            if self.config.get('ABS_PEAKS') and self.enough_data_for_alerting(point['dt']):
                                max_avg = max([x['metrics']['avg'] for x in self.clusters])
                                if self.prev_mva > max_avg:
                                    logging.warning('[%s][%s][%s] dropped spike with mva value bigger than known clusters max avg [%s..%s]',
                                                  self.uiname, self.metric, self.ident, self.cur_cluster_items[0]['dt'], point['dt'])
                                    # message='dropped spike with mva value bigger than known clusters max avg [point tz {}]'.format(point['dt'])
                            self.points_dropped += len(self.cur_cluster_items)
                            self.cur_cluster_items = []  # remove spike
            else:   # no value jump               
                # check if our current point value is way over historical avg
                if hist_window_avg_value and point['dt'] > (datetime.now() - timedelta(minutes=5)) \
                        and cur_mva > (hist_window_avg_value + (hist_window_avg_value * int(self.config.get('ABNORMAL_JUMP_THRESHOLD', 100)) / 100.0)):
                    logging.warning('[%s][%s][%s] historical avg (%s) exceeded by ABNORMAL_JUMP_THRESHOLD [%s %%]. cur_mva = %s',
                                 self.uiname, self.metric, self.ident, hist_window_avg_value, self.config.get('ABNORMAL_JUMP_THRESHOLD'), cur_mva)
                    # alert_message = 'historical avg {} exceeded by more than {}'.format(hist_window_avg_value, self.config.get('ABNORMAL_JUMP_THRESHOLD'))  # no alert sent currently
                
                # check against getting "0" values
                if self.config.get('NO_ZEROES') and len(self.cur_cluster_items) >= self.config['MIN_CLUSTER_ITEMS'] and cur_mva == 0:
                    logging.error('[%s][%s][%s] zero-line found [%s..%s]', self.uiname, self.metric, self.ident, self.cur_cluster_items[0]['dt'], point['dt'])
                    alert_message = 'zero-line found [{}..{}]'.format(self.cur_cluster_items[0]['dt'], point['dt']) # TODO no email sent currently
                    metrics = math_helpers.calculate_cluster_metrics([x['value'] for x in self.cur_cluster_items])
                    if self.graph_mode:
                        self.clusters.append({'items': self.cur_cluster_items, 'metrics': metrics})
                    else:   # save some RAM, store only first/last item as not drawing graphs
                        self.clusters.append({'items': [self.cur_cluster_items[0], self.cur_cluster_items[-1]], 'metrics': metrics})
                    self.cur_cluster_items = []  # remove spike


                point['breakout'] = False

        # cleanup of old clusters if needed
        if len(self.clusters) > 1 and (datetime.now() - self.clusters[0]['items'][-1]['dt']).total_seconds() > self.config['LOOKBACK_HISTORY_DAYS'] * 60 * 60 * 24:
            logging.debug('[%s][%s][%s] dropping old cluster with %s points from memory [%s..%s]', self.uiname, self.metric, self.ident,
                          len(self.clusters[0]['items']), self.clusters[0]['items'][0]['dt'], self.clusters[0]['items'][-1]['dt'])
            self.clusters.pop(0)

        self.prev_point = point
        self.prev_mva = cur_mva
        self.cur_cluster_items.append(point)
        logging.debug('[%s][%s][%s] added point to running pattern. new item count: %s', self.uiname, self.metric, self.ident, len(self.cur_cluster_items))
        if new_cluster_found and alert_message:
            return self.clusters_with_unknown_patterns[-1], alert_message
        elif alert_message:
            return None, alert_message
        else:
            return None, None
