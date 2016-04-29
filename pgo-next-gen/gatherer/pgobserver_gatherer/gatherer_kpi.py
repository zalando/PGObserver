from pgobserver_gatherer.gatherer_base import GathererBase
import pgobserver_gatherer.datadb as datadb
from pgobserver_gatherer.globalconfig import Datasets
from pgobserver_gatherer.globalconfig import SUPPORTED_DATASETS


class KPIGatherer(GathererBase):        # mixture of most important indicators - TPS, rollbacks, size diff etc
                                        # good for live/console mode
    def __init__(self, host_data, settings):
        GathererBase.__init__(self, host_data, settings, Datasets.KPI)
        self.interval_seconds = settings[SUPPORTED_DATASETS[Datasets.KPI][0]] * 60
        self.columns_to_store = ['kpi_timestamp', 'kpi_host_id', 'kpi_load_1min', 'kpi_active_backends',
                                 'kpi_blocked_backends', 'kpi_oldest_tx_s', 'kpi_tps', 'kpi_rollbacks', 'kpi_blks_read',
                                 'kpi_blks_hit', 'kpi_temp_bytes', 'kpi_wal_location_b', 'kpi_seq_scans', 'kpi_ins',
                                 'kpi_upd', 'kpi_del', 'kpi_sproc_calls', 'kpi_blk_read_time', 'kpi_blk_write_time',
                                 'kpi_deadlocks']
        self.datastore_table_name = 'monitor_data.kpi_data'

    def gather_data(self):
        sql_get = """
            WITH q_stat_tables AS (
              SELECT * FROM pg_stat_all_tables
              WHERE NOT schemaname LIKE any(array[E'pg\\_%'])
            ),
            q_iud_sums_persistent AS (
              SELECT
                sum(n_tup_ins) AS ins,
                sum(n_tup_upd) AS upd,
                sum(n_tup_del) AS del
              FROM
                q_stat_tables t
                JOIN
                pg_class c on c.oid = t.relid
                WHERE c.relpersistence = 'p'
            )
            SELECT
              {host_id} AS kpi_host_id,
              now() AS kpi_timestamp,
              {load_1min} AS kpi_load_1min,
              numbackends AS kpi_numbackends,
              (select count(1) from pg_stat_activity where datid = d.datid and state = 'active' and pid != pg_backend_pid()) AS kpi_active_backends,
              (select count(1) from pg_stat_activity where datid = d.datid and waiting) AS kpi_blocked_backends,
              (select round(extract(epoch from now()) - extract(epoch from (select xact_start from pg_stat_activity
                where datid = d.datid order by xact_start limit 1))))::int AS kpi_oldest_tx_s,  -- should filter out autovacuum?
              xact_commit + xact_rollback AS kpi_tps,
              xact_rollback AS kpi_rollbacks,
              blks_read AS kpi_blks_read,
              blks_hit AS kpi_blks_hit,
              temp_bytes AS kpi_temp_bytes,
              (select pg_xlog_location_diff(pg_current_xlog_location(), '0/0')) AS kpi_wal_location_b,
              (select sum(seq_scan) from q_stat_tables) AS kpi_seq_scans,
              ins AS kpi_ins,
              upd AS kpi_upd,
              del AS kpi_del,
              (select sum(calls) from pg_stat_user_functions where not schemaname like any(array[E'pg\\_%', 'information_schema'])) AS kpi_sproc_calls,
              blk_read_time AS kpi_blk_read_time,
              blk_write_time AS kpi_blk_write_time,
              deadlocks AS kpi_deadlocks
              --pg_database_size(d.datname) AS db_size_b
            FROM
              pg_stat_database d
              JOIN
              q_iud_sums_persistent on true
            WHERE
              datname = current_database()
            """

        return datadb.executeOnHost(self.host_data['host_name'], self.host_data['host_port'],
                                    self.host_data['host_db'], self.host_data['host_user'], self.host_data['host_password'],
                                    sql_get.format(host_id=self.host_id,
                                                   load_1min=('(select load_1min from zz_utils.get_load_average())' if self.have_zz_utils else 'null')))
