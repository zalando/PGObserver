/*
  Patch created on master "295914b" (23.03.2016)
  Patch that changes behaviour of the "performance views" so that instead of "host_name", "host_ui_shortname" is used as input.
  If using also "lock monitoring" (extra_features/blocking_monitor) re-run of extra_features/blocking_monitor/pgo_setup/30_analyzing_helpers.sql is needed.
*/

begin;

set search_path to monitor_data;

alter table hosts add constraint check_ui_shortname check (host_ui_shortname ~ '^[a-z|0-9]+$'); -- no spaces, hyphens etc

alter table monitor_data.perf_comparison_table_thresholds add pctt_host_id int not null references monitor_data.hosts(host_id);
update monitor_data.perf_comparison_table_thresholds p set pctt_host_id = host_id from hosts h where p.pctt_host_name = h.host_name;
alter table monitor_data.perf_comparison_table_thresholds drop constraint perf_comparison_table_thresholds_pkey;
alter table monitor_data.perf_comparison_table_thresholds add constraint perf_comparison_table_thresholds_pkey primary key (pctt_host_id, pctt_schema_name, pctt_table_name);



alter table monitor_data.perf_comparison_sproc_thresholds add pcst_host_id int references hosts (host_id) on delete cascade;
update monitor_data.perf_comparison_sproc_thresholds set pcst_host_id = host_id from hosts where host_name = pcst_host_name;
alter table monitor_data.perf_comparison_sproc_thresholds alter pcst_host_id set not null;
alter table monitor_data.perf_comparison_sproc_thresholds drop constraint perf_comparison_sproc_thresholds_pkey;
alter table monitor_data.perf_comparison_sproc_thresholds add constraint perf_comparison_sproc_thresholds_pkey primary key (pcst_host_id, pcst_sproc_name);

\i ../schema/03_1_perf_comparison_sprocs.sql

end;
