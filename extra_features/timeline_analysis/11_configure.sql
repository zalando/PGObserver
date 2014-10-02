
insert into  host_clusters select 16,13;
insert into  host_clusters select 16,14;
insert into  host_clusters select 16,15;
insert into  host_clusters select 16,16;
insert into  host_clusters select 20,17;
insert into  host_clusters select 20,18;
insert into  host_clusters select 20,19;
insert into  host_clusters select 20,20;


insert into shard_pairs select 3,10;
insert into shard_pairs select 10,11;
insert into shard_pairs select 11,12;
insert into shard_pairs select 12,3;
insert into shard_pairs select 41,42;
insert into shard_pairs select 42,43;
insert into shard_pairs select 43,44;
insert into shard_pairs select 44,41;

insert into shard_pairs select 13,14;
insert into shard_pairs select 14,15;
insert into shard_pairs select 15,16;
insert into shard_pairs select 16,17;
insert into shard_pairs select 17,18;
insert into shard_pairs select 18,19;
insert into shard_pairs select 19,20;
insert into shard_pairs select 20,13;


insert into shard_pairs select 8,107;

insert into shard_pairs select 98,99;
insert into shard_pairs select 99,100;
insert into shard_pairs select 100,101;
insert into shard_pairs select 101,102;
insert into shard_pairs select 102,103;
insert into shard_pairs select 103,104;
insert into shard_pairs select 104,105;
insert into shard_pairs select 105,98;


--truncate table monitoring_configuration;
--insert into monitoring_configuration select 'total_time_same_days_daily_past_samples','4', 'number of weeks back to average for comparison';
--insert into monitoring_configuration select 'total_time_same_days_daily_percent','30', 'the percent increase to trigger daily total time alert for procedures';
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

--insert into monitoring_configuration select 'number_of_runs_locks_loop','100', 'HOW MANY TIMES TO RUN THE LOCKS LOOP, 0 = infinate';



--insert into performance_ignore_list select 46,null;
insert into performance_ignore_list select 99,'payment_control_get_zgate_state';



