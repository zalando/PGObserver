from pgobserver_gatherer.gatherer_base import GathererBase
import pgobserver_gatherer.datadb as datadb
from pgobserver_gatherer.globalconfig import Datasets
from pgobserver_gatherer.globalconfig import SUPPORTED_DATASETS


class LoadGatherer(GathererBase):

    def __init__(self, host_data, settings):
        GathererBase.__init__(self, host_data, settings, Datasets.LOAD)
        self.interval_seconds = settings[SUPPORTED_DATASETS[Datasets.LOAD][0]] * 60
        self.columns_to_store = ['load_timestamp', 'load_host_id', 'load_1min_value', 'load_5min_value',
                            'load_15min_value', 'xlog_location', 'xlog_location_mb', 'xlog_location_b']
        self.datastore_table_name = 'monitor_data.host_load'

    def gather_data(self):
        sql_get = """
            with
            q_load as (
              select
                {load_avgs} as load,
                pg_current_xlog_location() as xlog_location
            )
            select
              now() as load_timestamp,
              (q_load.load[1]*100)::int as load_1min_value, -- in old Java gatherer for some reason it's multiplied by 100
              (q_load.load[2]*100)::int as load_5min_value,
              (q_load.load[3]*100)::int as load_15min_value,
              -- q_load.load[1]::double precision as load_1min_value,
              -- q_load.load[2]::double precision as load_5min_value,
              -- q_load.load[3]::double precision as load_15min_value,
              q_load.xlog_location,
              case
                when current_setting('server_version_num')::int >= 90200 then
                  pg_xlog_location_diff(q_load.xlog_location, '0/0')::int8
                else
                  null::int8
              end as xlog_location_b
            from
              q_load
        """
        data = datadb.executeOnHost(self.host_data['host_name'], self.host_data['host_port'],
                                    self.host_data['host_db'], self.host_data['host_user'], self.host_data['host_password'],
                                    sql_get.format(load_avgs=('(select array[min1, min5, min15]::numeric[] from zz_utils.get_load_average() t(min1, min5, min15))' if self.have_zz_utils else '(select array[null,null, null]::numeric[])')))

        for d in data:
            d['load_host_id'] = self.host_id
            if d['xlog_location_b'] is None:
                d['xlog_location_b'] = self.xlog_location_to_bytes(d['xlog_location'])
                d['xlog_location_mb'] = d['xlog_location_b'] / 1024**2

        return data

    @staticmethod
    def xlog_location_to_bytes(xlog_location):
        """"
        xlog_location - result from Postgres pg_current_xlog_location(), e.g. 2F1/CDABE000
        returns - long int of location converted to megabytes (assuming 1 WAL file = 16MB)

        from 9.2 there's also the pg_xlog_location_diff() function, which one should use
        """
        B_PER_WAL = 16 * 1024 * 1024
        if not xlog_location:
            return None
        splits = xlog_location.split('/')
        splits[1] = splits[1].zfill(8)  # pg_current_xlog_location can return 0/1644148 or 1/4240
        logical_segments = int(splits[0], 16)*256       # 00 - FF
        physical_segments = int(splits[1][0:2], 16)     # 00 - FF
        ret = int((logical_segments + physical_segments) * B_PER_WAL + int(splits[1][2:], 16))
        if ret < 0:
            raise Exception('Invalid xlog calculation: {} => {}'.format(xlog_location, ret))
        return ret
