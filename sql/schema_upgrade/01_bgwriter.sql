/*
 * should be executed with psql with pgobserver home directory as current working directory
 * */
BEGIN;

    SET search_path TO monitor_data, public;

    create or replace function set_setting_key(p_settings text, p_key text, p_value int)
    returns text
    as $$
    declare
      l_data text[];
      l_key_found boolean := false;
      c record;
      l_ret text;
    begin
      p_settings := trim(p_settings);
      if position('"'||p_key||'"' in p_settings) = 0 then
        --return regexp_replace(p_settings, E'(.*)\n}', format(E'\\1,\n"%s": %s\n}', p_key, p_value), 'g');
        return regexp_replace(p_settings, E'^{(.*\\d)\\s*}$', format(E'{\\1,\n"%s": %s\n}', p_key, p_value), 'g');
      else
        return regexp_replace(p_settings, format('"(%s)"\s?:\s?(\d+)', p_key), format('"\1": %s', p_value));
      end if;
    end;
    $$ language plpgsql;

    CREATE TABLE stat_bgwriter_data(
        sbd_timestamp                timestamp NOT NULL,
        sbd_host_id                  int NOT NULL,
        sbd_checkpoints_timed        bigint,
        sbd_checkpoints_req          bigint,
        sbd_checkpoint_write_time    double precision,
        sbd_checkpoint_sync_time     double precision,
        sbd_buffers_checkpoint       bigint,
        sbd_buffers_clean            bigint,
        sbd_maxwritten_clean         bigint,
        sbd_buffers_backend          bigint,
        sbd_buffers_backend_fsync    bigint,
        sbd_buffers_alloc            bigint,
        sbd_stats_reset              timestamp
    );

    CREATE UNIQUE INDEX ON stat_bgwriter_data (sbd_host_id, sbd_timestamp);

    SELECT monitor_data.create_partitioning_trigger_for_table('stat_bgwriter_data', 'sbd');

    /*
    append the configuration for bgstat gatherer to existing configuration for all hosts
    bgwriter should gather every 60 minutes
    **/

    UPDATE hosts
       SET host_settings = set_setting_key(host_settings, 'statBgwriterGatherInterval'::text, 60)
     WHERE host_settings not like '%statBgwriterGatherInterval%';


    \i sql/schema/04_monitor_api.sql


COMMIT;
