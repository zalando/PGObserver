import math_helpers

# run "py.test math_helpers.py" to test

CLUSTER_DIFFERENCE_CALC_WEIGHTS = {"avg": 20, "med": 10, "stddev": 10, "min": 15, "max": 15, "itemcount": 10, "trend": 20}


def test_calculate_cluster_metrics_trend_up():
    points = [1, 2, 4, 6, 8]
    print('points', points)
    metrics = math_helpers.calculate_cluster_metrics(points)
    print('metrics', metrics)
    assert metrics['trend'] == 'up'


def test_calculate_cluster_metrics_trend_down():
    points = [2, 3, 2, 1, 0]
    print('points', points)
    metrics = math_helpers.calculate_cluster_metrics(points)
    print('metrics', metrics)
    assert metrics['trend'] == 'down'


def test_calculate_cluster_metrics_trend_flat():
    points = [8, 6, 7.8]
    print('points', points)
    metrics = math_helpers.calculate_cluster_metrics(points)
    print('metrics', metrics)
    assert metrics['trend'] == 'flat'


def test_get_metrics_difference_pct():
    dict1 = {'key': 21}
    dict2 = {'key': 22}
    diff_pct = math_helpers.get_metrics_difference_pct(dict1, dict2, 'key')
    print('diff_pct', diff_pct)
    assert diff_pct == 4


def test_normalize():
    points = [1, 4]
    normalized = math_helpers.normalize(points)
    assert normalized == [0.2, 0.8]


def test_weighed_average():
    normalized_points = [0.2, 0.8]
    weights = [100, 0]
    assert 0.2 == math_helpers.weighed_average(normalized_points, weights)

    normalized_points = [0.2, 0.8]
    weights = [50, 50]
    assert 0.5 == math_helpers.weighed_average(normalized_points, weights)


def test_cluster_distance():
    cl1 = {'avg': 10, 'med': 9, 'min': 3, 'max': 16, 'itemcount': 6, 'trend': 'flat', 'stddev': 1}
    cl2 = {'avg': 10, 'med': 9, 'min': 3, 'max': 16, 'itemcount': 6, 'trend': 'flat', 'stddev': 1}
    d = math_helpers.calculate_cluster_distance_from_metrics(cl1, cl2, CLUSTER_DIFFERENCE_CALC_WEIGHTS)
    print('cluster_distance1', d)
    assert d == 0

    cl1 = {'avg': 10, 'med': 9, 'min': 3, 'max': 16, 'itemcount': 6, 'trend': 'up', 'stddev': 1}
    cl2 = {'avg': 10*100, 'med': 9*100, 'min': 3/100.0, 'max': 16*100, 'itemcount': 6*100, 'trend': 'down', 'stddev': 1}
    d = math_helpers.calculate_cluster_distance_from_metrics(cl1, cl2, CLUSTER_DIFFERENCE_CALC_WEIGHTS)
    print('cluster_distance2', d)
    assert d > 90   # TODO gives 7940.0 now. too much?

    cl1 = {'avg': 10, 'med': 9, 'min': 3, 'max': 16, 'itemcount': 6, 'trend': 'flat', 'stddev': 1}
    cl2 = {'avg': 11, 'med': 9, 'min': 3, 'max': 16, 'itemcount': 6, 'trend': 'flat', 'stddev': 1}
    d = math_helpers.calculate_cluster_distance_from_metrics(cl1, cl2, CLUSTER_DIFFERENCE_CALC_WEIGHTS)
    print('cluster_distance3', d)
    assert d < 3


def test_calculate_moving_average_for_last_x_items():
    assert 4 == math_helpers.calculate_moving_average_for_last_x_items([1, 2, 3, 4, 5], 3)


def test_calculate_moving_average_for_last_x_items2():
    assert 5 == math_helpers.calculate_moving_average_for_last_x_items([2, 4, 4, 4, 5, 5, 7, 9], len([2, 4, 4, 4, 5, 5, 7, 9]))


def test_calculate_std_dev():
    assert 2 == math_helpers.calculate_std_dev([2, 4, 4, 4, 5, 5, 7, 9])


def test_calculate_trend_flat():
    assert 'flat' == math_helpers.calculate_trend([20, 40, 40, 21])


def test_calculate_trend_up():
    assert 'up' == math_helpers.calculate_trend([20, 40, 40, 45])


def test_calculate_trend_down():
    assert 'down' == math_helpers.calculate_trend([20, 40, 37, 1])
