--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

CREATE ROLE pgobserver_frontend WITH LOGIN PASSWORD 'pgobserver_frontend';
CREATE ROLE pgobserver_gatherer WITH LOGIN PASSWORD 'pgobserver_gatherer';
CREATE ROLE pgobserver_owner;

CREATE SCHEMA log_file_data;
ALTER SCHEMA log_file_data OWNER TO pgobserver_owner;

ALTER DEFAULT PRIVILEGES FOR ROLE pgobserver_owner IN SCHEMA log_file_data GRANT SELECT ON TABLES to pgobserver_frontend;
ALTER DEFAULT PRIVILEGES FOR ROLE pgobserver_owner IN SCHEMA log_file_data GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES to pgobserver_gatherer;
ALTER DEFAULT PRIVILEGES FOR ROLE pgobserver_owner IN SCHEMA log_file_data GRANT USAGE ON SEQUENCES to pgobserver_gatherer;

CREATE SCHEMA monitor_data;
ALTER SCHEMA monitor_data OWNER TO pgobserver_owner;

ALTER DEFAULT PRIVILEGES FOR ROLE pgobserver_owner IN SCHEMA monitor_data GRANT SELECT ON TABLES to pgobserver_frontend;
ALTER DEFAULT PRIVILEGES FOR ROLE pgobserver_owner IN SCHEMA monitor_data GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES to pgobserver_gatherer;
ALTER DEFAULT PRIVILEGES FOR ROLE pgobserver_owner IN SCHEMA monitor_data GRANT USAGE ON SEQUENCES to pgobserver_gatherer;

GRANT USAGE ON SCHEMA monitor_data TO pgobserver_gatherer;
GRANT USAGE ON SCHEMA monitor_data TO pgobserver_frontend;

SET ROLE TO pgobserver_owner;

SET search_path = public, pg_catalog;

--
-- Name: get_noversion_name(text); Type: FUNCTION; Schema: public;
--

CREATE FUNCTION get_noversion_name(n text) RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $_$ SELECT substring ( $1 from 'z[a-z]{1,4}_api|z[a-z]{1,2}_data' ) $_$;

--
-- Name: group_date(timestamp without time zone, double precision); Type: FUNCTION; Schema: public;
--

CREATE FUNCTION group_date(d timestamp without time zone, m double precision) RETURNS timestamp without time zone
    LANGUAGE sql IMMUTABLE
    AS $_$
select date_trunc('hour'::text, $1) + floor(date_part('minute'::text, $1 ) / $2) * ($2 * '00:01:00'::interval)
$_$;


SET search_path = log_file_data, pg_catalog;

CREATE TABLE database_log (
    log_time timestamp(3) with time zone,
    user_name text,
    database_name text,
    process_id integer,
    connection_from text,
    session_id text NOT NULL,
    session_line_num bigint NOT NULL,
    command_tag text,
    session_start_time timestamp with time zone,
    virtual_transaction_id text,
    transaction_id bigint,
    error_severity text,
    sql_state_code text,
    message text,
    detail text,
    hint text,
    internal_query text,
    internal_query_pos integer,
    context text,
    query text,
    query_pos integer,
    location text,
    application_name text
);

SET search_path = monitor_data, pg_catalog;

CREATE TABLE host_groups (
    group_id serial,
    group_name text,
    primary key ( group_id )
);

CREATE TABLE hosts (
    host_id serial,
    host_name text,
    host_port integer,
    host_user text,
    host_password text,
    host_db text,
    host_settings text DEFAULT '{
"loadGatherInterval": 5,
"tableIoGatherInterval": 10,
"sprocGatherInterval": 5,
"tableStatsGatherInterval": 10,
"uiLongName":"longName",
"uiShortName":"lN"
}'::text NOT NULL,
    host_group_id integer,
    host_enabled boolean DEFAULT false NOT NULL,
    primary key ( host_id )
);


CREATE TABLE host_load (
    load_host_id integer not null references hosts(host_id),
    load_timestamp timestamp without time zone NOT NULL,
    load_1min_value integer,
    load_5min_value integer,
    load_15min_value integer,
    load_iowait_value bigint,
    load_system_value bigint
);

CREATE TABLE host_cpu_stats (
  cpu_host_id integer not null references hosts(host_id),
  cpu_timestamp timestamp not null,
  cpu_user bigint,
  cpu_system bigint,
  cpu_idle bigint,
  cpu_iowait bigint,
  cpu_irq bigint,
  cpu_softirq bigint
);

CREATE TABLE marked_events (
    event_id serial,
    event_start timestamp without time zone,
    event_end timestamp without time zone,
    event_name text,
    event_description text,
    event_author text,
    event_color text,
    primary key ( event_id )
);

CREATE TABLE queries (
    query_id serial,
    query text,
    primary key ( query_id )
);

CREATE TABLE schedules (
    sc_host_id integer NOT NULL references hosts(host_id),
    sc_query_id integer NOT NULL,
    sc_interval integer,
    sc_created timestamp without time zone DEFAULT now(),
    sc_last_ok timestamp without time zone,
    sc_last_try timestamp without time zone
);

CREATE TABLE sprocs (
    sproc_id serial,
    sproc_host_id integer not null references hosts(host_id),
    sproc_schema text,
    sproc_name text,
    primary key ( sproc_id )
);

CREATE TABLE sproc_performance_data (
    sp_timestamp timestamp without time zone DEFAULT now() NOT NULL,
    sp_sproc_id integer NOT NULL references sprocs(sproc_id),
    sp_calls bigint,
    sp_total_time bigint,
    sp_self_time bigint
);

CREATE TABLE tables (
    t_id serial,
    t_host_id integer,
    t_schema text,
    t_name text,
    primary key ( t_id )
);

CREATE TABLE table_io_data (
    tio_table_id integer not null references tables(t_id),
    tio_timestamp timestamp without time zone,
    tio_heap_read bigint,
    tio_heap_hit bigint,
    tio_idx_read bigint,
    tio_idx_hit bigint
);

CREATE TABLE table_size_data (
    tsd_table_id integer NOT NULL references tables(t_id),
    tsd_timestamp timestamp without time zone NOT NULL,
    tsd_table_size bigint,
    tsd_index_size bigint,
    tsd_seq_scans bigint,
    tsd_index_scans bigint,
    tsd_tup_ins bigint,
    tsd_tup_upd bigint,
    tsd_tup_del bigint,
    tsd_tup_hot_upd bigint
);

CREATE TABLE tags (
    tag_id integer NOT NULL,
    tag_name text,
    tag_color text
);

CREATE TABLE tag_members (
    tm_id serial,
    tm_tag_id integer not null,
    tm_sproc_name text,
    tm_table_name text,
    tm_schema text,
    primary key ( tm_id )
);

--
-- Name: v_sproc_data; Type: VIEW; Schema: monitor_data; Owner: postgres
--

CREATE VIEW v_sproc_data AS
    SELECT (SELECT sprocs.sproc_name FROM sprocs WHERE (sprocs.sproc_id = t.sp_sproc_id)) AS name, (date_trunc('hour'::text, t.sp_timestamp) + (floor((date_part('minute'::text, t.sp_timestamp) / (15)::double precision)) * '00:15:00'::interval)) AS xaxis, sum(t.delta_calls) AS d_calls, sum(t.delta_self_time) AS d_self_time, sum(t.delta_total_time) AS d_total_time FROM (SELECT sproc_performance_data.sp_timestamp, sproc_performance_data.sp_sproc_id, COALESCE((sproc_performance_data.sp_calls - lag(sproc_performance_data.sp_calls) OVER (PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp)), (0)::bigint) AS delta_calls, COALESCE((sproc_performance_data.sp_self_time - lag(sproc_performance_data.sp_self_time) OVER (PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp)), (0)::bigint) AS delta_self_time, COALESCE((sproc_performance_data.sp_total_time - lag(sproc_performance_data.sp_total_time) OVER (PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp)), (0)::bigint) AS delta_total_time FROM sproc_performance_data WHERE (sproc_performance_data.sp_sproc_id IN (SELECT sprocs.sproc_id FROM sprocs)) ORDER BY sproc_performance_data.sp_timestamp) t GROUP BY t.sp_sproc_id, (date_trunc('hour'::text, t.sp_timestamp) + (floor((date_part('minute'::text, t.sp_timestamp) / (15)::double precision)) * '00:15:00'::interval)) ORDER BY (date_trunc('hour'::text, t.sp_timestamp) + (floor((date_part('minute'::text, t.sp_timestamp) / (15)::double precision)) * '00:15:00'::interval));

--
-- Name: v_sproc_data2; Type: VIEW; Schema: monitor_data; Owner: postgres
--

CREATE VIEW v_sproc_data2 AS
    SELECT t.sp_sproc_id, min(t.sp_timestamp) AS min_sp, (date_trunc('hour'::text, t.sp_timestamp) + (floor((date_part('minute'::text, t.sp_timestamp) / (15)::double precision)) * '00:15:00'::interval)) AS xaxis, sum(t.delta_calls) AS d_calls, sum(t.delta_self_time) AS d_self_time, sum(t.delta_total_time) AS d_total_time FROM (SELECT sproc_performance_data.sp_timestamp, sproc_performance_data.sp_sproc_id, COALESCE((sproc_performance_data.sp_calls - lag(sproc_performance_data.sp_calls) OVER (PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp)), (0)::bigint) AS delta_calls, COALESCE((sproc_performance_data.sp_self_time - lag(sproc_performance_data.sp_self_time) OVER (PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp)), (0)::bigint) AS delta_self_time, COALESCE((sproc_performance_data.sp_total_time - lag(sproc_performance_data.sp_total_time) OVER (PARTITION BY sproc_performance_data.sp_sproc_id ORDER BY sproc_performance_data.sp_timestamp)), (0)::bigint) AS delta_total_time FROM sproc_performance_data) t GROUP BY t.sp_sproc_id, (date_trunc('hour'::text, t.sp_timestamp) + (floor((date_part('minute'::text, t.sp_timestamp) / (15)::double precision)) * '00:15:00'::interval));

SET search_path = monitor_data, pg_catalog;

CREATE INDEX host_load_load_timestamp_idx ON host_load USING btree (load_timestamp);

CREATE INDEX sproc_performance_data_sp_sproc_id_sp_timestamp_idx ON sproc_performance_data USING btree (sp_sproc_id, sp_timestamp);

CREATE INDEX sproc_performance_data_sp_timestamp_idx ON sproc_performance_data USING btree (sp_timestamp DESC);

ALTER TABLE sproc_performance_data CLUSTER ON sproc_performance_data_sp_timestamp_idx;

CREATE INDEX table_io_data_tio_table_id_tio_timestamp_idx ON table_io_data USING btree (tio_table_id, tio_timestamp);

CREATE INDEX table_io_data_tio_timestamp_idx ON table_io_data USING btree (tio_timestamp);

CREATE INDEX table_size_data_tsd_table_id_tsd_timestamp_idx ON table_size_data USING btree (tsd_table_id, tsd_timestamp);

CREATE INDEX table_size_data_tsd_table_id_tsd_timestamp_idx1 ON table_size_data USING btree (tsd_table_id, tsd_timestamp);

CREATE INDEX table_size_data_tsd_timestamp_idx ON table_size_data USING btree (tsd_timestamp);

ALTER TABLE table_size_data CLUSTER ON table_size_data_tsd_timestamp_idx;

CREATE INDEX tables_t_host_id_idx ON tables USING btree (t_host_id);

CREATE INDEX tag_members_tm_tag_id_idx ON tag_members USING btree (tm_tag_id);

CREATE INDEX ON host_cpu_stats ( cpu_host_id, cpu_timestamp );
