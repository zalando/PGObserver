import logging

from pgobserver_gatherer.gatherer_base import GathererBase
from pgobserver_gatherer.id_cache import IdCache
import pgobserver_gatherer.datadb as datadb
from pgobserver_gatherer.globalconfig import Datasets
from pgobserver_gatherer.globalconfig import SUPPORTED_DATASETS


class TableStatsGatherer(GathererBase):

    def __init__(self, host_data, settings):
        GathererBase.__init__(self, host_data, settings, Datasets.TABLE)
        self.interval_seconds = settings[SUPPORTED_DATASETS[Datasets.TABLE][0]] * 60
        self.columns_to_store = ['tsd_timestamp', 'tsd_host_id', 'tsd_table_id', 'tsd_table_size', 'tsd_index_size',
                                 'tsd_seq_scans', 'tsd_index_scans', 'tsd_tup_ins', 'tsd_tup_upd', 'tsd_tup_del',
                                 'tsd_tup_hot_upd']
        self.datastore_table_name = 'monitor_data.table_size_data'

        # gatherer specifics
        self.use_approximation = settings.get('useTableSizeApproximation') == 1

        # cache
        self.cache_table_name = 'monitor_data.tables'
        self.cache_id_column = 't_id'
        self.cache_host_id_column = 't_host_id'
        self.cache_key_columns = ['t_schema', 't_name']
        self.table_id_cache = IdCache(self.cache_table_name, self.cache_id_column, self.cache_key_columns,
                                               self.cache_host_id_column, self.host_id)

    def gather_data(self):
        data = []
        sql_get = '''
            SELECT
              now() as tsd_timestamp,
              {host_id} as tsd_host_id,
              schemaname as t_schema,
              relname as t_name,
              pg_table_size(relid) as tsd_table_size,
              pg_indexes_size(relid) as tsd_index_size,
              seq_scan as tsd_seq_scans,
              idx_scan as tsd_index_scans,
              n_tup_ins as tsd_tup_ins,
              n_tup_upd as tsd_tup_upd,
              n_tup_hot_upd as tsd_tup_hot_upd,
              n_tup_del as tsd_tup_del
            FROM
              pg_stat_user_tables
            WHERE
              NOT schemaname LIKE ANY (array[E'tmp%', E'temp%', E'%api\\_r%'])
              --AND relname = 't1'
            '''.format(host_id=self.host_id)
        if self.use_approximation:
            sql_get = '''
                SELECT
                  now() as tsd_timestamp,
                  {host_id} as tsd_host_id,
                  schemaname as t_schema,
                  ut.relname as t_name,
                  (c.relpages + coalesce(ctd.relpages,0) + cti.relpages)::int8 * 8192 as tsd_table_size,
                  (select sum(relpages) from pg_class ci join pg_index on indexrelid =  ci.oid where indrelid = c.oid)::int8 * 8192 as tsd_index_size,
                  seq_scan as tsd_seq_scans
                  # idx_scan as tsd_index_scans,
                  # n_tup_ins as tsd_tup_ins,
                  # n_tup_upd as tsd_tup_upd,
                  # n_tup_hot_upd as tsd_tup_hot_upd,
                  # n_tup_del as tsd_tup_del
                FROM
                  pg_stat_user_tables ut
                  JOIN
                  pg_class c ON c.oid = ut.relid
                  LEFT JOIN
                  pg_class ctd ON ctd.oid = c.reltoastrelid
                  LEFT JOIN
                  pg_class cti ON cti.oid = ctd.reltoastidxid
                WHERE
                  not ut.schemaname like any (array[E'tmp%', E'temp%', E'%api\\_r%'])
                '''.format(host_id=self.host_id)

        try:
            data = datadb.executeOnHost(self.host_data['host_name'], self.host_data['host_port'],
                                        self.host_data['host_db'], self.host_data['host_user'], self.host_data['host_password'],
                                        sql_get)
        except Exception as e:
            # TODO will fail on >=9.4, needs a recurive query as pg_class.reltoastidxid is gone
            # https://github.com/postgres/postgres/commit/2ef085d0e6960f5087c97266a7211d37ddaa9f68
            if 'reltoastidxid does not exist' in str(e):
                logging.error('[%s][%s] - useTableSizeApproximation not available for >9.4', self.host_name, self.gatherer_name)
                return []   # do not write to retry queue
            else:
                raise e

        return data

    def store_data(self, data):
        logging.info('[%s][%s] running custom store_data() for %s rows', self.host_name, self.gatherer_name, len(data))

        if len(self.table_id_cache.cache) == 0:
            self.table_id_cache.refresh_from_db()

        new_tables = [x for x in data if not self.table_id_cache.has((x['t_schema'], x['t_name']))]
        logging.debug('[%s][%s] %s new tables found', self.host_name, self.gatherer_name, len(new_tables))

        if new_tables:
            for x in new_tables:
                self.table_id_cache.put((x['t_schema'], x['t_name']))

        for d in data:
            d['tsd_table_id'] = self.table_id_cache.get((d['t_schema'], d['t_name']))

        super().store_data(data)
