from pgobserver_gatherer.gatherer_base import GathererBase
import pgobserver_gatherer.datadb as datadb
from pgobserver_gatherer.globalconfig import Datasets
from pgobserver_gatherer.globalconfig import SUPPORTED_DATASETS


class BgwriterGatherer(GathererBase):

    def __init__(self, host_data, settings):
        GathererBase.__init__(self, host_data, settings, Datasets.BGWRITER)
        self.interval_seconds = settings[SUPPORTED_DATASETS[Datasets.BGWRITER][0]] * 60
        self.columns_to_store = ['sbd_timestamp', 'sbd_host_id', 'sbd_checkpoints_timed', 'sbd_checkpoints_req',
                            'sbd_checkpoint_write_time', 'sbd_checkpoint_sync_time', 'sbd_buffers_checkpoint',
                            'sbd_buffers_clean', 'sbd_maxwritten_clean', 'sbd_buffers_backend',
                            'sbd_buffers_backend_fsync', 'sbd_buffers_alloc', 'sbd_stats_reset']
        self.datastore_table_name = 'monitor_data.stat_bgwriter_data'

    def gather_data(self):
        sql_get = '''SELECT
                       now() as sbd_timestamp,
                       %s as sbd_host_id,
                       checkpoints_timed as sbd_checkpoints_timed,
                       checkpoints_req as sbd_checkpoints_req,
                       checkpoint_write_time as sbd_checkpoint_write_time,
                       checkpoint_sync_time as sbd_checkpoint_sync_time,
                       buffers_checkpoint as sbd_buffers_checkpoint,
                       buffers_clean as sbd_buffers_clean,
                       maxwritten_clean as sbd_maxwritten_clean,
                       buffers_backend as sbd_buffers_backend,
                       buffers_backend_fsync as sbd_buffers_backend_fsync,
                       buffers_alloc as sbd_buffers_alloc,
                       stats_reset as sbd_stats_reset
                     from
                       pg_stat_bgwriter
                        '''

        data = datadb.executeOnHost(self.host_data['host_name'], self.host_data['host_port'],
                                    self.host_data['host_db'], self.host_data['host_user'], self.host_data['host_password'],
                                    sql_get, (self.host_id,))

        return data
