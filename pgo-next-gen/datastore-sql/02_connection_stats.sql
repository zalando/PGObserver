SET search_path TO monitor_data, public;
SET ROLE TO pgobserver_gatherer;

BEGIN;

    CREATE TABLE stat_connection_data(
        scd_timestamp                timestamp NOT NULL,
        scd_host_id                  int NOT NULL,
        scd_total                    int,   -- is in stat_database_data.sdd_numbackends also actually
        scd_active                   int,
        scd_waiting                  int,
        scd_longest_session_seconds  int,
        scd_longest_tx_seconds       int,
        scd_longest_query_seconds    int
    );

    CREATE INDEX ON stat_connection_data (scd_timestamp);

    SELECT monitor_data.create_partitioning_trigger_for_table('stat_connection_data', 'scd');

    /*
    -- optional - append the configuration for all active hosts with 10min interval
    UPDATE hosts
       SET host_settings = set_setting_key(host_settings, 'statConnectionGatherInterval'::text, 10)
     WHERE host_enabled AND host_settings NOT LIKE '%statConnectionGatherInterval%';
    */


COMMIT;
