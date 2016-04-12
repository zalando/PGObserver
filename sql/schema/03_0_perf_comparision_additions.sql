/*
  here are tables/procedures that enable reporting of peaks in sequential scans or sproc average runtimes

  used in frontend:
    /perftables
    /perfapi
    /perfindexes
*/

SET role TO pgobserver_gatherer;
SET search_path = monitor_data, public;

/*
sproc report tuning tables
*/

drop table if exists perf_comparison_default_sproc_thresholds;

create table perf_comparison_default_sproc_thresholds (
pcdst_allowed_runtime_growth_pct numeric,
pcdst_allowed_callcount_growth_pct numeric, --not used currently
pcdst_share_on_total_runtime_pct numeric,
pcdst_min_reported_call_count bigint
);

insert into perf_comparison_default_sproc_thresholds
select 25, 500,2,100;


drop table if exists perf_comparison_sproc_thresholds;
--for fine tuning of sprocs
create table perf_comparison_sproc_thresholds (
pcst_host_id int not null,
pcst_sproc_name text not null,
pcst_host_name text, --deprecated
pcst_allowed_runtime_growth_pct numeric,
pcst_allowed_call_count_growth_pct numeric,
primary key (pcst_host_id, pcst_sproc_name)
);






/*
tables
*/

drop table if exists perf_comparison_default_tables_thresholds;
--pct are for one day
create table perf_comparison_default_tables_thresholds (
pcdtt_allowed_seq_scan_pct numeric,
pcdtt_allowed_size_growth bigint,
pcdtt_allowed_size_growth_pct numeric,
pcdtt_min_reported_table_size_threshold bigint,
pcdtt_min_reported_scan_count bigint
);
insert into perf_comparison_default_tables_thresholds
select 50, null, 10, 50*1000*1000, 50;


drop table if exists perf_comparison_table_thresholds;
--fine tuning of tables
create table perf_comparison_table_thresholds (
pctt_host_id int not null references monitor_data.hosts(host_id),
pctt_schema_name text not null,
pctt_table_name text not null,
pctt_host_name text,    -- deprecated
pctt_allowed_seq_scan_count bigint,
pctt_allowed_seq_scan_pct numeric,
pctt_allowed_size_growth bigint, --bytes
pctt_allowed_size_growth_pct numeric,
primary key (pctt_host_id, pctt_schema_name, pctt_table_name)
);


drop table if exists perf_comparison_schemas_ignored;

create table perf_comparison_schemas_ignored(
pcsi_schema text not null primary key
);


drop table if exists perf_comparison_tables_ignored;

create table perf_comparison_tables_ignored(
pcti_schema text not null,
pcti_table text not null,
primary key (pcti_schema, pcti_table)
);


/*
indexes
*/

drop table if exists perf_indexes_thresholds;

create table perf_indexes_thresholds (
pit_min_size_to_report numeric,
pit_max_scans_to_report numeric
);
insert into perf_indexes_thresholds
select 100*1000*1000, 5;


