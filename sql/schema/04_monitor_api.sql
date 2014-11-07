RESET ROLE;
CREATE SCHEMA monitor_api AUTHORIZATION pgobserver_gatherer;
SET ROLE TO pgobserver_gatherer;

SET SEARCH_PATH = monitor_api, public;
SET ROLE TO pgobserver_gatherer;

CREATE TYPE bgwriter_value AS(
    host_id int,
    log_timestamp timestamp,
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
    INSERT INTO monitor_data.stat_bgwriter_data
    (sbd_host_id,sbd_timestamp,sbd_checkpoints_timed,sbd_checkpoints_req,
    sbd_checkpoint_write_time,sbd_checkpoint_sync_time,sbd_buffers_checkpoint,
    sbd_buffers_clean,sbd_maxwritten_clean,sbd_buffers_backend,
    sbd_buffers_backend_fsync,sbd_buffers_alloc,sbd_stats_reset)
    VALUES
    (bgstat.host_id,bgstat.log_timestamp,bgstat.checkpoints_timed,bgstat.checkpoints_req,
    bgstat.checkpoint_write_time,bgstat.checkpoint_sync_time,bgstat.buffers_checkpoint,
    bgstat.buffers_clean,bgstat.maxwritten_clean,bgstat.buffers_backend,
    bgstat.buffers_backend_fsync,bgstat.buffers_alloc,bgstat.stats_reset)
    ;
$BODY$;
