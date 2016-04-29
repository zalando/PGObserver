SET search_path TO monitor_data, public;
SET ROLE TO pgobserver_gatherer;

BEGIN;

    CREATE TABLE kpi_data(
        kpi_timestamp           timestamp NOT NULL,
        kpi_host_id             int NOT NULL,
        kpi_numbackends         int,
        kpi_active_backends     int,
        kpi_blocked_backends    int,
        kpi_oldest_tx_s         int,
        kpi_tps                 int8,
        kpi_rollbacks           int8,
        kpi_blks_read           int8,
        kpi_blks_hit            int8,
        kpi_temp_bytes          int8,
        kpi_wal_location_b      int8,
        kpi_seq_scans           int8,
        kpi_ins                 int8,
        kpi_upd                 int8,
        kpi_del                 int8,
        kpi_sproc_calls         int8,
        kpi_deadlocks           int8,
        kpi_load_1min           float8,
        kpi_blk_read_time       float8,
        kpi_blk_write_time      float8
    );

    CREATE INDEX ON kpi_data(kpi_timestamp);

    SELECT monitor_data.create_partitioning_trigger_for_table('kpi_data', 'kpi');


    /*
    -- optional - append the configuration for all active hosts with 10min interval
    UPDATE hosts
       SET host_settings = set_setting_key(host_settings, 'KPIGatherInterval'::text, 5)
     WHERE host_enabled AND host_settings NOT LIKE '%KPIGatherInterval%';
    */


COMMIT;
