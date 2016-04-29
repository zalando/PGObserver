from pgobserver_gatherer.gatherer_base import GathererBase
import pgobserver_gatherer.datadb as datadb
from pgobserver_gatherer.globalconfig import Datasets
from pgobserver_gatherer.globalconfig import SUPPORTED_DATASETS


class DatabaseGatherer(GathererBase):

    def __init__(self, host_data, settings):
        GathererBase.__init__(self, host_data, settings, Datasets.DB)
        self.interval_seconds = settings[SUPPORTED_DATASETS[Datasets.DB][0]] * 60
        self.columns_to_store = ['sdd_host_id', 'sdd_timestamp', 'sdd_numbackends', 'sdd_xact_commit', 'sdd_xact_rollback',
                                 'sdd_blks_read', 'sdd_blks_hit', 'sdd_temp_files', 'sdd_temp_bytes', 'sdd_deadlocks',
                                 'sdd_blk_read_time', 'sdd_blk_write_time']
        self.datastore_table_name = 'monitor_data.stat_database_data'

    def gather_data(self):
        sql_91 = '''select
                      %s as sdd_host_id,
                      now() as sdd_timestamp,
                      numbackends as sdd_numbackends,
                      xact_commit as sdd_xact_commit,
                      xact_rollback as sdd_xact_rollback,
                      blks_read as sdd_blks_read,
                      blks_hit as sdd_blks_hit,
                      null as sdd_temp_files,
                      null as sdd_temp_bytes,
                      deadlocks as sdd_deadlocks,
                      blk_read_time::int8 as sdd_blk_read_time,
                      blk_write_time::int8 as sdd_blk_write_time
                    from
                      pg_stat_database
                    where
                      datname = current_database()
                        '''
        sql_92 = '''select
                      %s as sdd_host_id,
                      now() as sdd_timestamp,
                      numbackends as sdd_numbackends,
                      xact_commit as sdd_xact_commit,
                      xact_rollback as sdd_xact_rollback,
                      blks_read as sdd_blks_read,
                      blks_hit as sdd_blks_hit,
                      temp_files as sdd_temp_files,
                      temp_bytes as sdd_temp_bytes,
                      deadlocks as sdd_deadlocks,
                      blk_read_time::int8 as sdd_blk_read_time,
                      blk_write_time::int8 as sdd_blk_write_time
                    from
                      pg_stat_database
                    where
                      datname = current_database()
                        '''

        return datadb.executeOnHost(self.host_data['host_name'], self.host_data['host_port'],
                                    self.host_data['host_db'], self.host_data['host_user'], self.host_data['host_password'],
                                    sql_91 if self.pg_server_version_num < 90200 else sql_92, (self.host_id,))
