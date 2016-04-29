SET search_path TO monitor_data, public;
SET ROLE TO pgobserver_gatherer;

BEGIN;


    -- Schema changes

    -- Table for holding default gatherer setup
    CREATE TABLE default_host_settings (settings text not null);
    CREATE UNIQUE INDEX ON default_host_settings ((1));
    INSERT INTO default_host_settings SELECT
        '{
              "loadGatherInterval": 5,
              "tableIoGatherInterval": 10,
              "sprocGatherInterval": 5,
              "tableStatsGatherInterval": 10,
              "indexStatsGatherInterval": 20,
              "schemaStatsGatherInterval": 120,
              "blockingStatsGatherInterval": 0,
              "statStatementsGatherInterval": 0,
              "statDatabaseGatherInterval": 5,
              "KPIGatherInterval": 5,
              "statConnectionGatherInterval": 5,
              "useTableSizeApproximation": 0}
        ';

    ALTER TABLE hosts ALTER host_settings drop default;
    ALTER TABLE hosts ALTER host_settings drop not null;    -- if null then "default_host_settings.settings" will be used

    --ALTER TABLE hosts ALTER host_settings TYPE jsonb USING host_settings::jsonb; should move to JSONB as default at some point

    ALTER TABLE host_load ADD xlog_location_b int8;
--    ALTER TABLE host_load ALTER load_1min_value TYPE float;
--    ALTER TABLE host_load ALTER load_5min_value TYPE float;
--    ALTER TABLE host_load ALTER load_15min_value TYPE float;

COMMIT;