SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

CREATE ROLE pgobserver_frontend WITH LOGIN PASSWORD 'pgobserver_frontend';
CREATE ROLE pgobserver_gatherer WITH LOGIN PASSWORD 'pgobserver_gatherer';


CREATE SCHEMA monitor_data AUTHORIZATION pgobserver_gatherer;

DO $$
BEGIN
  EXECUTE 'ALTER DATABASE ' || current_database() || ' SET search_path = monitor_data, public';
END;
$$;

GRANT USAGE ON SCHEMA monitor_data TO pgobserver_frontend;
ALTER DEFAULT PRIVILEGES FOR ROLE pgobserver_gatherer IN SCHEMA monitor_data GRANT SELECT ON TABLES to pgobserver_frontend;


create extension if not exists pg_trgm;
--create extension if not exists btree_gin;

SET ROLE TO pgobserver_gatherer;

SET search_path = public, pg_catalog;



/*
    can be used to trunc timestamps by even quarter hours
*/

CREATE OR REPLACE FUNCTION group_date(d timestamp without time zone, m double precision) RETURNS timestamp without time zone
    LANGUAGE sql IMMUTABLE
    AS $_$
select date_trunc('hour'::text, $1) + floor(date_part('minute'::text, $1 ) / $2) * ($2 * '00:01:00'::interval)
$_$;



SET search_path = monitor_data, public;

CREATE TABLE host_groups (
    group_id serial,
    group_name text unique not null,
    primary key ( group_id )
);
insert into host_groups select 0, 'General';

CREATE TABLE hosts (
    host_id serial,
    host_name text not null,
    host_port integer not null default 5432,
    host_user text not null default 'pgobserver_gatherer',
    host_password text not null default 'to_be_replaced_manually', --PS when adding users via /hosts screen the the last enabled host's user/pw will be used
    host_db text not null,
    host_settings text not null DEFAULT '{
      "loadGatherInterval": 5,
      "tableIoGatherInterval": 10,
      "sprocGatherInterval": 5,
      "tableStatsGatherInterval": 10,
      "indexStatsGatherInterval": 20,
      "schemaStatsGatherInterval": 120,
      "blockingStatsGatherInterval": 0,
      "statStatementsGatherInterval": 0,
      "statDatabaseGatherInterval": 5}'::text NOT NULL,
    host_group_id integer not null default 0,
    host_enabled boolean NOT NULL DEFAULT true,
    host_gather_group text default 'gatherer1' not null, --makes multiple java gatherers possible
    host_db_export_name text,
    host_created timestamp not null default now(),
    host_last_modified timestamp not null default now(),
    host_ui_shortname text not null,
    host_ui_longname text not null,
    primary key ( host_id )
);
create unique index on hosts(host_name, host_port, host_db);
create unique index on hosts(host_ui_shortname);
create unique index on hosts(host_ui_longName);
GRANT INSERT, UPDATE ON hosts to pgobserver_frontend;
GRANT USAGE ON SEQUENCE hosts_host_id_seq to pgobserver_frontend;

create or replace function trigger_host_modified()
returns trigger as
$$
begin
  if NEW.host_last_modified = OLD.host_last_modified then
    NEW.host_last_modified = now();
  end if;
  return NEW;
end;
$$ language plpgsql;

create trigger hosts_set_last_modified before update on hosts
for each row execute procedure trigger_host_modified();



CREATE TABLE host_load (
    load_host_id integer not null references hosts(host_id),
    load_timestamp timestamp without time zone NOT NULL,
    load_1min_value integer,
    load_5min_value integer,
    load_15min_value integer,
    xlog_location text,
    xlog_location_mb bigint
);
CREATE INDEX ON host_load (load_timestamp);


/*
 list of patterns determining which schemas are to be monitored by the Java gatherer. PS and "and" operation will be performed on patterns
    - scmc_is_allowed='t' - include a pattern
    - scmc_is_allowed='n' - exclude a pattern
*/
CREATE TABLE sproc_schemas_monitoring_configuration (
 scmc_host_id int not null,
 scmc_schema_name_pattern text not null,
 scmc_is_pattern_included boolean not null,
 primary key (scmc_host_id, scmc_schema_name_pattern)
);

/*
  default config is marked by id = 0 and will be used if host doesn't have it's own config.
  if you're not using weekly api-s then something like that would probably make sense
  PS from weekly api schmas (E'_api_r[_0-9]+') only last 2 are used
*/
insert into sproc_schemas_monitoring_configuration values
    (0, E'public', 'f'),
    (0, E'pg\\_%', 'f'),
    (0, E'information_schema', 'f'),
    (0, E'tmp%', 'f'),
    (0, E'temp%', 'f');

/*
-- Zalando usecase
insert into sproc_schemas_monitoring_configuration values
  (0, E'%\\_api%', 't'),
  (0, E'%\\_data', 't');
*/
insert into sproc_schemas_monitoring_configuration values
  (0, E'%', 't');

CREATE TABLE sprocs (
    sproc_id serial,
    sproc_host_id integer not null references hosts(host_id),
    sproc_schema text,
    sproc_name text,
    sproc_created timestamp without time zone default now(),
    primary key ( sproc_id )
);
CREATE INDEX ON monitor_data.sprocs USING gin (sproc_name gin_trgm_ops);


CREATE TABLE sproc_performance_data (
    sp_timestamp timestamp without time zone DEFAULT now() NOT NULL,
    sp_host_id int,
    sp_sproc_id integer NOT NULL references sprocs(sproc_id),
    sp_calls bigint,
    sp_total_time bigint,
    sp_self_time bigint
);
CREATE INDEX ON sproc_performance_data (sp_timestamp);


CREATE TABLE tables (
    t_id serial,
    t_host_id integer,
    t_schema text,
    t_name text,
    primary key ( t_id )
);
CREATE INDEX ON tables (t_host_id);


CREATE TABLE table_io_data (
    tio_timestamp timestamp without time zone,
    tio_host_id int NOT NULL references hosts(host_id),
    tio_table_id integer not null references tables(t_id),
    tio_heap_read bigint,
    tio_heap_hit bigint,
    tio_idx_read bigint,
    tio_idx_hit bigint
);
CREATE INDEX ON table_io_data (tio_table_id, tio_timestamp);
CREATE INDEX ON table_io_data (tio_timestamp);


CREATE TABLE table_size_data (
    tsd_timestamp timestamp without time zone NOT NULL,
    tsd_host_id integer NOT NULL references hosts(host_id),
    tsd_table_id integer NOT NULL references tables(t_id),
    tsd_table_size bigint,
    tsd_index_size bigint,
    tsd_seq_scans bigint,
    tsd_index_scans bigint,
    tsd_tup_ins bigint,
    tsd_tup_upd bigint,
    tsd_tup_del bigint,
    tsd_tup_hot_upd bigint
);
CREATE INDEX ON table_size_data (tsd_table_id, tsd_timestamp);
CREATE INDEX ON table_size_data (tsd_timestamp);


CREATE TABLE indexes (
    i_id serial not null,
    i_host_id integer not null,
    i_schema text not null,
    i_table_name text not null,
    i_name text not null,
    i_created timestamp default now(),
    primary key ( i_id )
);

CREATE UNIQUE INDEX ON indexes(i_host_id, i_schema, i_table_name, i_name);

CREATE TABLE index_usage_data (
    iud_timestamp timestamp not null default clock_timestamp(),
    iud_host_id int NOT NULL references hosts(host_id),
    iud_index_id int not null references indexes(i_id),
    iud_scan int8 not null,
    iud_tup_read int8 not null,
    iud_tup_fetch int8 not null,
    iud_size int8 not null
);

create index on index_usage_data ( iud_index_id, iud_timestamp );


CREATE TABLE schema_usage_data (
    sud_timestamp timestamp not null,
    sud_host_id int not null references hosts(host_id),
    sud_schema_name text not null,
    sud_sproc_calls int8 not null,
    sud_seq_scans int8 not null,
    sud_idx_scans int8 not null,
    sud_tup_ins int8 not null,
    sud_tup_upd int8 not null,
    sud_tup_del int8 not null
);
create index on schema_usage_data(sud_host_id, sud_schema_name);
create index on schema_usage_data(sud_timestamp);



create table stat_statements_data(
ssd_timestamp timestamp not null,
ssd_host_id int not null,
ssd_query text not null,
ssd_query_id int8 not null,
ssd_calls int8,
ssd_total_time int8,
ssd_blks_read int8,
ssd_blks_written int8,
ssd_temp_blks_read int8,
ssd_temp_blks_written int8
);
create index on stat_statements_data(ssd_host_id, ssd_timestamp);


create table stat_database_data(
sdd_timestamp timestamp not null,
sdd_host_id int not null,
sdd_numbackends int,
sdd_xact_commit int8,
sdd_xact_rollback int8,
sdd_blks_read int8,
sdd_blks_hit int8,
sdd_temp_files int8,
sdd_temp_bytes int8,
sdd_deadlocks int8,
sdd_blk_read_time int8,
sdd_blk_write_time int8
);
create index on stat_database_data(sdd_host_id, sdd_timestamp);

