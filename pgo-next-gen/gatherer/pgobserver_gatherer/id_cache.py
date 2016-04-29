import logging

import pgobserver_gatherer.datadb as datadb
import pgobserver_gatherer.datastore_adapter as datastore_adapter
from pgobserver_gatherer.globalconfig import SUPPORTED_DATASETS


class IdCache(object):  # TODO do writes via datastore adapter for testability

    def __init__(self, table_name, id_column_name, key_columns: list, host_id_column_name, host_id):
        self.table_name = table_name
        self.id_column = id_column_name
        self.key_columns = key_columns  # columns to be inserted
        self.cache = {}
        self.host_id = host_id
        self.host_id_column = host_id_column_name

    def refresh_from_db(self):
        sql = 'select {cols} from {table} where {host_id_col} = %s'
        cols = ', '.join(self.key_columns) + ', ' + self.id_column
        sql = sql.format(cols=cols, table=self.table_name, host_id_col=self.host_id_column)
        data = datadb.execute(sql, (self.host_id,))
        self.cache.clear()
        for d in data:
            l = []
            for x in self.key_columns:
                l.append(d[x])
            self.cache[tuple(l)] = d[self.id_column]
        logging.debug('IdCache: refreshed for [%s][%s] with %s items', self.host_id, self.table_name, len(self.cache))

    def get(self, key_tuple):
        if len(self.cache) == 0:
            self.refresh_from_db()
        return self.cache.get(key_tuple)

    def has(self, key_tuple):
        return key_tuple in self.cache

    def put(self, key_column_values: tuple):
        if len(key_column_values) != len(self.key_columns):
            raise Exception('IdCache - key column length doesnt match: ' + str(key_column_values))
        key_names = self.key_columns + [self.host_id_column]
        key_values = key_column_values + (self.host_id,)
        data = dict(zip(key_names, key_values))
        ret_data = datastore_adapter.store_to_postgres_metrics_db([data], self.table_name,
                                                                  key_names, [self.id_column])
        id = ret_data[0][self.id_column]
        self.cache[tuple(key_column_values)] = id
        return id

    # def put_bulk(self): TODO


if __name__ == '__main__':
    c = IdCache('sprocs', 'sproc_id', ['sproc_host_id', 'sproc_schema'], 'sp_host_id', 1)
    print(c.cache)
    c.put([1, 's'])
    print(c.cache)
