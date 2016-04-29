import logging
import math
from datetime import datetime
from datetime import timedelta


def calculate_moving_average_for_last_x_items(values, last_items_to_consider, weights=None):
    if len(values) < last_items_to_consider:
        raise Exception('input too short!')
    if weights:
        if len(weights) != last_items_to_consider:
            logging.error('len(weights): %s, last_items_to_consider: %s', len(weights), last_items_to_consider)
            raise Exception('incorrect weights!')
        return weighed_average(values[-last_items_to_consider:], weights)
    return float(sum(values[-last_items_to_consider:])) / last_items_to_consider


def calculate_std_dev(points):      # TODO DataFrame.std ?
    avg = calculate_moving_average_for_last_x_items(points, len(points))
    dev_sqr = sum([(x - avg)**2 for x in points])
    return round(math.sqrt(dev_sqr / float(len(points))), 1)


def calculate_trend(points):
    if points[0] > points[-1] * 1.10:
        return 'down'
    elif points[0] < points[-1] * 0.90:
        return 'up'
    else:               # +/- 10% counts as flat
        return 'flat'


def calculate_cluster_metrics(points):
    # min, max, med, avg, trend=up|down|flat
    ret = {}

    point_count = len(points)
    ret['min'] = min(points)
    ret['max'] = max(points)
    ret['avg'] = sum(points) / point_count
    ret['itemcount'] = len(points)
    ret['stddev'] = calculate_std_dev(points)

    # median
    points_sorted = sorted(points)
    if point_count % 2 == 1:
        ret['med'] = points_sorted[int(point_count / 2)]
    else:
        ret['med'] = (points_sorted[int(point_count / 2)-1] + points_sorted[int(point_count / 2)]) / 2

    # trend
    ret['trend'] = calculate_trend(points)

    return ret


def get_metrics_difference_pct(dict1, dict2, key):  # should be absolute difference, not dependent of order
    if dict1[key] == 0 and dict2[key] == 0:
        return 0
    pct = abs(dict1[key] - dict2[key]) / (float(dict1[key] + dict2[key]) / 2.0)
    return int(pct * 100)


def normalize(values):  # moves all values to range 0..1
    print('normalize():values=', values)
    temp_vals = [1 if x < 1 else x for x in values]     # giving always 1% of deviation
    val_sum = sum(temp_vals)
    return [float(x)/val_sum for x in temp_vals]


def weighed_average(values, weights):
    logging.debug('weighed_average(): values_normalized=%s, weights=%s', values, weights)
    weigh_sum = 0
    for v, w in zip(values, weights):
        weigh_sum += v * w
    logging.debug('weighed_average(): weigh_sum=%s, sum(weights)=%s', weigh_sum, sum(weights))
    return weigh_sum / sum(weights)


def calculate_cluster_distance_from_metrics(cluster1, cluster2, weights_dict):
    """
    Returns an avg pct  difference
    # Returns a number from 0..1, 0 meaning nearly identical clusters
    # Metrics included: min/max, avg, median, itemcount
    """
    if sum(weights_dict.values()) != 100:
        raise Exception('Unbalanced weights! sum = {}'.format(sum(weights_dict.values())))

    diffs = []
    weights = []
    for k, w in weights_dict.items():
        if k == 'trend':
            if cluster1[k] == cluster2[k]:
                diff_pct = 0
            elif cluster1[k] == 'flat' or cluster2[k] == 'flat':
                diff_pct = 50
            else:
                diff_pct = 100
        else:
            logging.debug('calculate_cluster_distance_from_metrics - v1: %s, v2: %s, weight: %s', cluster1[k], cluster2[k], w)
            diff_pct = get_metrics_difference_pct(cluster1, cluster2, k)
        logging.debug('diff_pct: %s', diff_pct)
        diffs.append(diff_pct)
        weights.append(w)

    logging.debug('diffs: %s', diffs)
    logging.debug('weights: %s', weights)
    if sum(diffs) == 0:
        return 0
    wa = weighed_average(diffs, weights)
    return wa


class TimeSeriesAverageCalculator:
    """A ring-buffer dropping values older than window_age_seconds and returns avg value if age reached (else None)"""

    def __init__(self, window_age_seconds, no_zeroes=False):
        if type(window_age_seconds) != int:
            raise Exception('window_age_seconds needs to be an int')
        self.max_age_seconds = window_age_seconds
        self.val_ring = []
        self.dt_ring = []
        self.sum = 0
        self.max_age_reached = False
        self.no_zeroes = no_zeroes

    def add_point(self, value, dt):
        if value is None or (self.no_zeroes and value == 0):
            return None
        if self.dt_ring and dt <= self.dt_ring[0]:
            raise Exception('dt <= last dt')

        self.sum += value
        self.dt_ring.append(dt)
        self.val_ring.append(value)

        if self.dt_ring[0] < (dt - timedelta(seconds=self.max_age_seconds)):
            if not self.max_age_reached:
                self.max_age_reached = True
            while self.dt_ring and self.dt_ring[0] < dt - timedelta(seconds=self.max_age_seconds):
                self.dt_ring.pop(0)
                self.sum -= self.val_ring.pop(0)

        if self.max_age_reached:
            return round(self.sum / len(self.val_ring)) if self.dt_ring else None
        else:
            return None


if __name__ == '__main__':
    tsac = TimeSeriesAverageCalculator(3)
    n = datetime.now() - timedelta(seconds=15)
    for i, x in enumerate(range(10)):
        print tsac.add_point(2, n + timedelta(seconds=i))
