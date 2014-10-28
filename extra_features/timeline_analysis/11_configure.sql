/* default run-time configuration values */

--truncate table monitoring_configuration;

insert into monitoring_configuration select 'total_time_same_days_hourly_past_samples','4', 'number of weeks back to average for comparison';
insert into monitoring_configuration select 'total_time_same_days_hourly_percent','30', 'the percent increase to trigger hourly total time alert for procedures';
insert into monitoring_configuration select 'total_time_same_days_threshold','1000000', 'minimum number of ms/hour deemed interesting';
insert into monitoring_configuration select 'total_time_same_days_hourly_percent_for_totals','50', 'the percent increase to trigger hourly total FOR THE WHOLE HOST TOTAL for procedures';

insert into monitoring_configuration select 'total_time_same_days_daily_tbl_past_samples','4', 'number of weeks back to average for comparison';
insert into monitoring_configuration select 'total_time_same_days_daily_tbl_percent','30', 'the percent increase to trigger daily total time alert for talbes';



insert into monitoring_configuration select 'total_hosts_same_days_past_samples','4', 'number of weeks back to average for comparison of buffer cache misses';
insert into monitoring_configuration select 'total_hosts_same_days_percent','30', 'the percent increase to trigger daily total time alert for buffer cache misses';
insert into monitoring_configuration select 'total_hosts_same_days_threshold','1000000', 'minimum number of cache misses deemed interesting';


insert into monitoring_configuration select 'size_same_days_past_samples','4', 'number of weeks back to average for comparison of sizes / scans';
insert into monitoring_configuration select 'size_same_days_percent','30', 'the percent increase of size / scans to trigger alert';
insert into monitoring_configuration select 'size_same_days_threshold','1000000', 'minimum table size deemed interesting';
insert into monitoring_configuration select 'scans_same_days_threshold','10000', 'minimum scans deemed interesting';


insert into monitoring_configuration select 'report_max_higher_values','3', 'how many higher values will muffle a suspect high value';
insert into monitoring_configuration select 'report_past_days_to_check','7', 'how many days back to check to see if value is new high';


insert into monitoring_configuration select 'shard_compare_min_total_time','1000000', 'do not check sprocs running less than 1000 seconds per day';
insert into monitoring_configuration select 'shard_compare_min_tbl_scans','100000', 'do not check tables with less than 100,000 scans a day';
insert into monitoring_configuration select 'shard_compare_min_factor_to_report','10', 'report only if a shard is TEN times worse than the other';


/* setup of which pairs of hosts should be combined/compared for the "shard" analysis */

-- insert into host_clusters ...
-- insert into shard_pairs ...

/* case some functions behave totally chaotic */
--insert into performance_ignore_list ..



