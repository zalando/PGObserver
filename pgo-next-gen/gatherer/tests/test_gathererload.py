from pgobserver_gatherer.gatherer_load import LoadGatherer


def test_xlog_location_to_mb():
    assert LoadGatherer.xlog_location_to_bytes('2F1/CDABE000') == 3237560967168
    assert LoadGatherer.xlog_location_to_bytes('0/1644148') == 23347528
    assert LoadGatherer.xlog_location_to_bytes('0/4240') == 16960
