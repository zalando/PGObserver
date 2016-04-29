from pgobserver_gatherer.gatherer_base import GathererBase
import pgobserver_gatherer.datadb as datadb
from pgobserver_gatherer.globalconfig import Datasets
from pgobserver_gatherer.globalconfig import SUPPORTED_DATASETS


class SchemaStatsGatherer(GathererBase):

    def __init__(self, host_data, settings):
        GathererBase.__init__(self, host_data, settings, Datasets.SCHEMAS)
        self.interval_seconds = settings[SUPPORTED_DATASETS[Datasets.SCHEMAS][0]] * 60
        self.columns_to_store = ['sud_timestamp', 'sud_host_id', 'sud_schema_name', 'sud_sproc_calls', 'sud_seq_scans',
                                 'sud_idx_scans', 'sud_tup_ins', 'sud_tup_upd', 'sud_tup_del']
        self.datastore_table_name = 'monitor_data.schema_usage_data'

    def gather_data(self):
        sql_get = '''
            with q_sproc_calls as (

                  select
                        nspname as schemaname,
                        coalesce(sum(calls),0) as sproc_calls
                  from
                        pg_namespace n
                        left join
                        pg_stat_user_functions f on f.schemaname = n.nspname
                  where
                        not nspname like any (array['pg_toast%', 'pg_temp%' ,'pgq%', '%utils%'])
                        and nspname not in ('pg_catalog', 'information_schema', '_v', 'zz_utils', 'zz_commons')
                  group by
                        nspname
            ),
            q_table_stats as (
                  select
                        nspname as schemaname,
                        sum(seq_scan) as seq_scan,
                        sum(idx_scan) as idx_scan,
                        sum(n_tup_ins) as n_tup_ins,
                        sum(n_tup_upd) as n_tup_upd,
                        sum(n_tup_del) as n_tup_del
                  from
                        pg_namespace n
                        left join
                        pg_stat_all_tables t on t.schemaname = n.nspname
                  where
                        not nspname like any (array['pg_toast%', 'pg_temp%' ,'pgq%'])
                        and nspname not in ('pg_catalog', 'information_schema', '_v', 'zz_utils', 'zz_commons')
                  group by
                        nspname
            )
            select
                  now() as sud_timestamp,
                  {host_id} as sud_host_id,
                  coalesce(t.schemaname,p.schemaname) as sud_schema_name,
                  coalesce(p.sproc_calls,0) as sud_sproc_calls,
                  coalesce(t.seq_scan,0) as sud_seq_scans,
                  coalesce(t.idx_scan,0) as sud_idx_scans,
                  coalesce(t.n_tup_ins,0) as sud_tup_ins,
                  coalesce(t.n_tup_upd,0) as sud_tup_upd,
                  coalesce(t.n_tup_del,0) as sud_tup_del
            from
                  q_table_stats t
                  full outer join
                  q_sproc_calls p on p.schemaname = t.schemaname
            order by
                  2;
            '''.format(host_id=self.host_id)

        data = datadb.executeOnHost(self.host_data['host_name'], self.host_data['host_port'],
                                    self.host_data['host_db'], self.host_data['host_user'], self.host_data['host_password'],
                                    sql_get)
        return data


