import re

from pgobserver_gatherer.gatherer_base import GathererBase
import pgobserver_gatherer.datadb as datadb
from pgobserver_gatherer.globalconfig import Datasets
from pgobserver_gatherer.globalconfig import SUPPORTED_DATASETS


QUERY_CLEANUP_REGEX = re.compile(r'\s+')


class BlockingLocksGatherer(GathererBase):

    def __init__(self, host_data, settings):
        GathererBase.__init__(self, host_data, settings, Datasets.LOCKS)
        self.interval_seconds = settings[SUPPORTED_DATASETS[Datasets.LOCKS][0]] * 60
        # self.columns_to_store_locks = ['bl_host_id', 'bl_timestamp', 'locktype', 'database', 'relation',
        #                          'page', 'tuple', 'virtualxid', 'transactionid', 'classid', 'objid', 'objsubid',
        #                          'virtualtransaction', 'pid', 'mode', 'granted', 'fastpath']
        # self.datastore_table_name_locks = 'monitor_data.blocking_locks'
        self.columns_to_store_processes = ['bp_host_id', 'bp_timestamp', 'datid', 'datname', 'pid',
                                 'usesysid', 'usename', 'application_name', 'client_addr', 'client_hostname',
                                 'client_port', 'backend_start', 'xact_start', 'query_start', 'state_change',
                                 'waiting', 'state', 'query']
        self.datastore_table_name_processes = 'monitor_data.blocking_processes'

    def gather_data(self):
        latest_timestamps = self.get_latest_timestamps()
        data_locks = self.gather_data_locks(latest_timestamps['locks'])
        data_processes = self.gather_data_processes(latest_timestamps['processes'])
        if data_locks or data_processes:
            return [(data_locks, data_processes)]
        else:
            return []

    def store_data(self, data):
        data_locks, data_processes = data[0]
        if data_locks or data_processes:
            # TODO should be 1 tx as if 2nd insert fails the whole dataset is pushed to retry queue currently
            if data_locks:
                super().store_data(data_locks, self.columns_to_store_locks, self.datastore_table_name_locks)
            if data_processes:
                super().store_data(data_processes, self.columns_to_store_processes, self.datastore_table_name_processes)

    def get_latest_timestamps(self):    # this data is cumulative on hosts so we need to keep track
        sql = '''
            select coalesce((select bp_timestamp from monitor_data.blocking_processes
                                where bp_host_id = %s order by bp_timestamp desc limit 1),
                             now() - '1 days'::interval) as tz
            union all
            select coalesce((select bl_timestamp from monitor_data.blocking_locks
                                where bl_host_id = %s order by bl_timestamp desc limit 1),
                            now() - '1 days'::interval) as tz
        '''
        data = datadb.execute(sql, (self.host_id, self.host_id))
        return {'processes': data[0]['tz'], 'locks': data[1]['tz']}

    def gather_data_locks(self, timestamp_from):
        return []   # this information is not used in the frontend so point to transfer
        # sql_get_locks = '''
        #         with t as (
        #         select
        #         *,
        #         %s as bl_host_id
        #         from z_blocking.blocking_locks where bl_timestamp > %s
        #         )
        #         select * from t where not granted
        #         union all
        #         select t1.*
        #         from t t1
        #         where
        #         (t1.granted and exists
        #          (select 1 from t t2
        #           where t2.bl_timestamp=t1.bl_timestamp
        #             and t2.pid != t1.pid
        #             and
        #             (
        #               not t2.granted
        #               and
        #               (  (t1.transactionid is not null and t1.transactionid = t2.transactionid)
        #               or
        #               (t1.virtualxid is not null and t1.virtualxid = t2.virtualxid)
        #               or (t1.classid is not null and t1.classid  = t2.classid and t1.objid = t2.objid and t1.objsubid = t2.objsubid)
        #               or (t1.database is not null and t1.database = t2.database and t1.relation = t2.relation)
        #               )
        #             )
        #           )
        #         )
        #         order by bl_timestamp
        #     '''
        # return datadb.executeOnHost(self.host_data['host_name'], self.host_data['host_port'],
        #                             self.host_data['host_db'], self.host_data['host_user'], self.host_data['host_password'],
        #                             sql_get_locks, (self.host_id, timestamp_from))

    def gather_data_processes(self, timestamp_from):
        sql_get_processes = '''
            select
              %s as bp_host_id,
              bp_timestamp,
              datid::int,
              datname,
              pid,
              usesysid::int,
              usename::text,
              application_name,
              client_addr,
              client_hostname,
              client_port,
              backend_start::text,
              xact_start::text,
              query_start::text,
              state_change::text,
              waiting,
              state,
              query
            from
              z_blocking.blocking_processes
            where
              bp_timestamp > %s
            order by
              bp_timestamp
        '''

        data = datadb.executeOnHost(self.host_data['host_name'], self.host_data['host_port'],
                                    self.host_data['host_db'], self.host_data['host_user'], self.host_data['host_password'],
                                    sql_get_processes, (self.host_id, timestamp_from))

        for d in data:
            d['query'] = QUERY_CLEANUP_REGEX.sub(' ', d['query'])

        return data
