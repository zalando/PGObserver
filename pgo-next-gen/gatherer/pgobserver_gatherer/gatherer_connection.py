from pgobserver_gatherer.gatherer_base import GathererBase
import pgobserver_gatherer.datadb as datadb
from pgobserver_gatherer.globalconfig import Datasets
from pgobserver_gatherer.globalconfig import SUPPORTED_DATASETS


class ConnectionStatsGatherer(GathererBase):

    def __init__(self, host_data, settings):
        GathererBase.__init__(self, host_data, settings, Datasets.CONNS)
        self.interval_seconds = settings[SUPPORTED_DATASETS[Datasets.CONNS][0]] * 60
        self.columns_to_store = ['scd_timestamp', 'scd_host_id', 'scd_total', 'scd_active',
                                 'scd_waiting', 'scd_longest_session_seconds', 'scd_longest_tx_seconds',
                                 'scd_longest_query_seconds']
        self.datastore_table_name = 'monitor_data.stat_connection_data'
        # TODO <9.2 ver support

    def gather_data(self):
        sql_get = '''
        with sa_snapshot as (
          select * from pg_stat_activity where pid != pg_backend_pid() and not query like 'autovacuum:%'
        )
        select
          now() as scd_timestamp,
          {host_id} as scd_host_id,
          (select count(*) from sa_snapshot) as scd_total,
          (select count(*) from sa_snapshot where state = 'active') as scd_active,
          (select count(*) from sa_snapshot where waiting) as scd_waiting,
          (select extract(epoch from (now() - backend_start))::int
            from sa_snapshot order by backend_start limit 1) as scd_longest_session_seconds,
          (select extract(epoch from (now() - xact_start))::int
            from sa_snapshot where xact_start is not null order by xact_start limit 1) as scd_longest_tx_seconds,
          (select extract(epoch from max(now() - query_start))::int
            from sa_snapshot where state = 'active') as scd_longest_query_seconds
        '''.format(host_id=self.host_id)

        data = datadb.executeOnHost(self.host_data['host_name'], self.host_data['host_port'],
                                    self.host_data['host_db'], self.host_data['host_user'], self.host_data['host_password'],
                                    sql_get)

        return data
