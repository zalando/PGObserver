import logging

from pgobserver_gatherer.gatherer_base import GathererBase
from pgobserver_gatherer.id_cache import IdCache
import pgobserver_gatherer.datadb as datadb
from pgobserver_gatherer.globalconfig import Datasets
from pgobserver_gatherer.globalconfig import SUPPORTED_DATASETS


class IndexGatherer(GathererBase):

    def __init__(self, host_data, settings):
        GathererBase.__init__(self, host_data, settings, Datasets.INDEX)
        self.interval_seconds = settings[SUPPORTED_DATASETS[Datasets.INDEX][0]] * 60
        self.columns_to_store = ['iud_timestamp', 'iud_host_id', 'iud_index_id', 'iud_scan',
                                 'iud_tup_read', 'iud_tup_fetch', 'iud_size']
        self.datastore_table_name = 'monitor_data.index_usage_data'
        # gatherer specifics
        self.use_approximation = settings.get('useTableSizeApproximation') == 1
        # cache
        self.cache_table_name = 'monitor_data.indexes'
        self.cache_id_column = 'i_id'
        self.cache_host_id_column = 'i_host_id'
        self.cache_key_columns = ['i_schema', 'i_table_name', 'i_name']
        self.index_id_cache = IdCache(self.cache_table_name, self.cache_id_column, self.cache_key_columns,
                                               self.cache_host_id_column, self.host_id)

    def gather_data(self):
        sql_get = '''
            SELECT
              now() as iud_timestamp,
              {host_id} as iud_host_id,
              schemaname as i_schema,
              relname as i_table_name,
              indexrelname as i_name,
              coalesce(idx_scan, 0) as iud_scan,
              coalesce(idx_tup_read, 0) as iud_tup_read,
              coalesce(idx_tup_fetch, 0) as iud_tup_fetch,
              coalesce(pg_table_size(indexrelid), 0) as iud_size
            FROM
              pg_stat_user_indexes
            WHERE
              indexrelname not like E'tmp%'
              AND schemaname not like E'%api\\_r%'
              AND indexrelname = 't1_c1_idx'
            ORDER BY
              i_schema, i_name
            '''.format(host_id=self.host_id)
        if self.use_approximation:
            sql_get = '''
                SELECT
                  now() as iud_timestamp,
                  {host_id} as iud_host_id,
                  schemaname as i_schema,
                  relname as i_table_name,
                  indexrelname as i_name,
                  coalesce(idx_scan, 0) as iud_scan,
                  coalesce(idx_tup_read, 0) as iud_tup_read,
                  coalesce(idx_tup_fetch, 0) as iud_tup_fetch,
                  (select coalesce(relpages, 0) from pg_class where oid = indexrelid)::int8 * 8192 as iud_size
                FROM
                  pg_stat_user_indexes
                WHERE
                  indexrelname not like E'tmp%'
                  and schemaname not like E'%api\\_r%'
                ORDER BY
                  i_schema, i_name
                '''.format(host_id=self.host_id)

        data = datadb.executeOnHost(self.host_data['host_name'], self.host_data['host_port'],
                                    self.host_data['host_db'], self.host_data['host_user'], self.host_data['host_password'],
                                    sql_get)
        return data

    def store_data(self, data):
        logging.info('[%s][%s] running custom store_data() for %s rows', self.host_name, self.gatherer_name, len(data))

        if len(self.index_id_cache.cache) == 0:
            self.index_id_cache.refresh_from_db()

        new_indexes = [x for x in data if not self.index_id_cache.has((x['i_schema'], x['i_schema'], x['i_name']))]
        logging.info('[%s][%s] %s new indexes found', self.host_name, self.gatherer_name, len(new_indexes))

        if new_indexes:
            for x in new_indexes:
                self.index_id_cache.put((x['i_schema'], x['i_schema'], x['i_name']))

        for d in data:
            d['iud_index_id'] = self.index_id_cache.get((d['i_schema'], d['i_schema'], d['i_name']))

        super().store_data(data)
