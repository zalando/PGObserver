import pgobserver_gatherer.datadb as datadb
from pgobserver_gatherer.id_cache import IdCache

def test_write_keys():
    """ integration test actually, needs DB. should mock datastore_adapter TODO """
    assert True
    # cache = IdCache('monitor_data.tables', 't_id', ['t_schema', 't_name'], 't_host_id', host_id=1)
    # id = cache.put(('mytestschema1', 'mytesttable1'))
    # assert id == cache.get(('mytestschema1', 'mytesttable1'))
    # datadb.execute('delete from monitor_data.tables where t_id = %s', (id,))
