CREATE SCHEMA monitor_api AUTHORIZATION pgobserver_gatherer;

SET SEARCH_PATH = monitor_api, public;
SET ROLE TO pgobserver_gatherer;

CREATE TYPE bgwriter_value AS(
    host_id int,
    sb_timestamp timestamp,
    checkpoints_timed bigint,
    checkpoints_req bigint,
    checkpoint_write_time double precision,
    checkpoint_sync_time double precision,
    buffers_checkpoint bigint,
    buffers_clean bigint,
    maxwritten_clean bigint,
    buffers_backend bigint,
    buffers_backend_fsync bigint,
    buffers_alloc bigint,
    stats_reset timestamp
);

CREATE OR REPLACE FUNCTION save_bgwriter_stats(IN bgstat bgwriter_value)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
AS $BODY$
    INSERT INTO monitor_data.stat_bgwriter
    (sb_host_id,sb_timestamp,sb_checkpoints_timed,sb_checkpoints_req,
    sb_checkpoint_write_time,sb_checkpoint_sync_time,sb_buffers_checkpoint,
    sb_buffers_clean,sb_maxwritten_clean,sb_buffers_backend,
    sb_buffers_backend_fsync,sb_buffers_alloc,sb_stats_reset)
    VALUES
    (bgstat.host_id,bgstat.sb_timestamp,bgstat.checkpoints_timed,bgstat.checkpoints_req,
    bgstat.checkpoint_write_time,bgstat.checkpoint_sync_time,bgstat.buffers_checkpoint,
    bgstat.buffers_clean,bgstat.maxwritten_clean,bgstat.buffers_backend,
    bgstat.buffers_backend_fsync,bgstat.buffers_alloc,bgstat.stats_reset)
    ;
$BODY$;
