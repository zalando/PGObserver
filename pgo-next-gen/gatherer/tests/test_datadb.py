import pgobserver_gatherer.datadb as datadb


def test_local_execute():
    datadb.init_connection_pool(datadb.connection_string)
    data = datadb.execute('select 1 as x')
    assert data[0]['x'] == 1
