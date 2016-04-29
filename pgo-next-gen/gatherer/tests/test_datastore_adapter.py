import pgobserver_gatherer.datastore_adapter as datastore_adapter


def test_split_dict_data_to_tuples_for_given_columns():
    data_as_dicts = [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]
    data = datastore_adapter.split_dict_data_to_tuples_for_given_columns(data_as_dicts, ['a', 'b'])
    assert data == [(1, 2), (3, 4)]


def test_add_to_local_database():
    data = [{'sproc_host_id': 1, 'sproc_schema': """asda\tasd"''s""", 'sproc_name': 'vsdas%&SD#¤'}]
    datastore_adapter.store_to_postgres_metrics_db(data=data, table_name='sprocs',
                                                   insert_column_names=['sproc_host_id', 'sproc_schema', 'sproc_name'],
                                                   return_column_names=[])
