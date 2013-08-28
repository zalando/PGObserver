SET search_path = monitor_data, public;

/*
sprocs
*/

drop table if exists perf_comparison_sproc_thresholds;

create table perf_comparison_sproc_thresholds (
pcst_host_name text primary key,
pcst_allowed_runtime_growth_percentage numeric, --percentages
pcst_allowed_total_runtime_growth bigint,
pcst_allowed_call_count_growth_percentage numeric --percentages
);

/*
insert into perf_comparison_sproc_thresholds
select 'catalog1.db.zalando', 25, 1000, 100, null;
*/

drop table if exists perf_comparison_default_sproc_thresholds;

create table perf_comparison_default_sproc_thresholds ( --weekly
pcdst_allowed_runtime_growth_percentage numeric,
pcdst_allowed_callcount_growth_percentage numeric, --not used currently
pcdst_share_on_total_runtime_percentage numeric,
pcdst_share_on_total_runtime_percentage numeric,
pcdst_min_reported_call_count bigint
);

insert into perf_comparison_default_sproc_thresholds
select 25, 500,0.5,100;



/*
tables

todo, should start checking for unused indexes also? no per index data in pgmon currently...
*/

--todo - def. vals into coalesce + left join

drop table if exists perf_comparison_thresholds_tables;

create table perf_comparison_thresholds_tables (
pctt_host_name text,
pctt_schema_name text,
pctt_table_name text,
pctt_allowed_seq_scan_count bigint, --for week
pctt_allowed_seq_scan_percentage numeric, --for week
pctt_allowed_size_growth bigint, --bytes, for week
pctt_allowed_size_growth_percentage numeric, --for week
primary key (pctt_host_name, pctt_schema_name, pctt_table_name)
);

insert into perf_comparison_thresholds_tables
select 'catalog1.db.zalando', 'zcat_data', 'price_current',null,50,50;
insert into perf_comparison_thresholds_tables
select 'catalog1.db.zalando', 'zcat_data', 'price_definition',null,50,50;



drop table if exists perf_comparison_ignored_tables;

create table perf_comparison_ignored_tables (
pcit_schema text,
pcit_table text,
primary key (pcit_schema, pcit_table)
);

insert into perf_comparison_ignored_tables
select 'zcat_data', 'price_definition';


drop table if exists perf_comparison_default_tables_thresholds;
--weekly
create table perf_comparison_default_tables_thresholds (
pcdtt_allowed_seq_scan_percentage numeric,
pcdtt_allowed_size_growth bigint, --not used
pcdtt_allowed_size_growth_percentage numeric,
pcdtt_min_reported_table_size_threshold bigint,
pcdtt_min_reported_scan_count bigint
);
insert into perf_comparison_default_tables_thresholds
select 50, null, 10, 50*1000*1000, 50;






/*

sprocs api

*/

--select * from get_sproc_threshold_sinners_for_release('catalog1.db.zalando','r13_00_24','r13_00_25')
--drop function get_sproc_threshold_sinners_for_release(text,text,text)

create or replace function get_sproc_threshold_sinners_for_release(
          in p_host_name text
        , in p_release1 text
        , in p_release2 text
        , out host_id int
        , out host_name text
        , out sproc_schema text
        , out sproc_name text
        , out calltime_change_pct numeric
        , out share_on_total_runtime numeric
        , out execution_avg1 numeric
        , out execution_avg2 numeric
        , out calls1 bigint
        , out calls2 bigint
        , out callscount_change_pct numeric
        , out allowed_runtime_growth_pct numeric
        , out allowed_share_on_total_runtime_pct numeric
)
returns setof record
as $$

select
  host_id
, host_name
, sproc_schema
, split_part(sproc_name, '(', 1) as sproc_name --drops the input params listing
, calltime_change_pct
, share_on_total_runtime
, exec_avg1
, exec_avg2
, calls1
, calls2
, callscount_change_pct
, allowed_runtime_growth_percentage
, allowed_share_on_total_runtime_percentage
from
(
    select
          b.host_name
        , b.host_id
        , sproc_schema
        , sproc_name
        , case when exec_avg1 = 0 then 0 else round(((exec_avg2-exec_avg1)/exec_avg1::numeric*100),2) end as calltime_change_pct
        , case when global_total_time = 0 then 0 else round(proc_total_time/global_total_time::numeric*100, 2) end as share_on_total_runtime
        , case when calls1 = 0 then 0 else round((calls2-calls1)/calls1::numeric*100, 2) end as callscount_change_pct
        , exec_avg1
        , exec_avg2
        , calls1
        , calls2
        , coalesce (pcst_allowed_runtime_growth_percentage, pcdst_allowed_runtime_growth_percentage) as allowed_runtime_growth_percentage
        , coalesce (pcst_allowed_runtime_growth_percentage, pcdst_allowed_runtime_growth_percentage) as calltime_threshold_pct
        , global_total_time
        , pcdst_share_on_total_runtime_percentage as allowed_share_on_total_runtime_percentage
    from (

      select
            host_name
          , host_id
          , sproc_name
          , sproc_schema
          , lag(exec_avg) over (partition by host_name, split_part(sproc_schema, '_api_r', 1) , sproc_name order by host_name, sproc_schema, sproc_name) as exec_avg1
          , exec_avg as exec_avg2
          , lag(calls) over (partition by host_name, split_part(sproc_schema, '_api_r', 1) , sproc_name order by host_name, sproc_schema, sproc_name) as calls1
          , calls as calls2
          , proc_total_time
          , global_total_time
      from (
          select
              host_name
            , host_id
            , sproc_schema
            , sproc_name
            , round(max(sp_total_time)/max(sp_calls)::numeric, 2) AS exec_avg
            , max(sp_calls) as calls
            , max(sp_total_time) as proc_total_time
            , sum(max(sp_total_time)) over(partition by sproc_schema) as global_total_time
           from sproc_performance_data
              , sprocs
              , hosts
          where extract(dow from sp_timestamp) IN(1,2,3,4,5)
            and (sproc_schema like '%'|| $2 or sproc_schema like '%' || $3)
            and sp_sproc_id = sproc_id
            and ($1 = 'all' or host_name = $1)
            and sproc_host_id = host_id
            and sp_timestamp >= (select min(created) from sprocs where sproc_schema like '%'|| $2)
         group by host_name, host_id, sproc_schema, sproc_name
       ) a
     ) b
     join perf_comparison_default_sproc_thresholds on true -- 1 row
     left join perf_comparison_sproc_thresholds on pcst_host_name = host_name --custom thresholds
     where ($1 = 'all' or b.host_name = $1)
     and exec_avg1 is not null
) c
where calltime_change_pct >= allowed_runtime_growth_percentage
and share_on_total_runtime >= allowed_share_on_total_runtime_percentage
order by 5 desc, 2

$$ language sql set work_mem = '256MB';






--select * from get_sproc_threshold_sinners_for_period('catalog1.db.zalando','2013-07-01','2013-07-04')
--drop function get_sproc_threshold_sinners_for_period(text,timestamp,timestamp)

create or replace function get_sproc_threshold_sinners_for_period(
          in p_host_name text
        , in p_date1 timestamp
        , in p_date2 timestamp
        , out host_name text
        , out sproc_name text
        , out avg_arr numeric[]
        , out calls_arr bigint[]
        , out calltime_change int
        , out is_slower boolean
        , out callscount_change int
        , out calltime_threshold int
)
returns setof record
as $$

--todo

$$ language sql;












/*

tables api

*/


--select * from get_table_threshold_sinners_for_period('catalog1.db.zalando','2013-07-01','2013-07-04')
--select * from get_table_threshold_sinners_for_period('catalog1.db.zalando','2013-07-01','2013-07-04')
--drop function get_table_threshold_sinners_for_period(text,timestamp,timestamp)

create or replace function get_table_threshold_sinners_for_period(
          in p_host_name text
        , in p_date1 date
        , in p_date2 date
        , out host_id int
        , out host_name text
        , out schema_name text
        , out table_name text
        , out day date
        , out scan_change_percentage numeric
        , out scans1 bigint
        , out scans2 bigint
        , out size1 text
        , out size2 text
        , out size_change_percentage numeric
        , out allowed_seq_scan_percentage numeric
)
returns setof record
as $$

--set work_mem='128MB'



select
      host_id
    , host_name
    , schema_name
    , table_name
    , day
    , scan_change_percentage
    , scans1
    , scans2
    , pg_size_pretty(size1) as size1
    , pg_size_pretty(size2) as size2
    , size_change_percentage
    , allowed_seq_scan_percentage
--    , allowed_size_growth_percentage
--    , gloal_scans_on_day
from (

select
      host_name
    , schema_name
    , host_id
    , table_name
    , day
    , scans1
    , scans2
    , case when scans1 = 0 then 0 else round((scans2-scans1)/scans1::numeric*100,2) end as scan_change_percentage
    , size1
    , size2
    , case when size1 = 0 then 0 else round((size2-size1)/size1::numeric*100,2) end as size_change_percentage
    , pcdtt_allowed_seq_scan_percentage as allowed_seq_scan_percentage
    , pcdtt_allowed_size_growth_percentage as allowed_size_growth_percentage
    , pcdtt_min_reported_table_size_threshold as min_reported_table_size_threshold
    , pcdtt_min_reported_scan_count as min_reported_scan_count
    , global_scans_on_day
from
(
  select
      day
    , host_name
    , host_id
    , schema_name
    , table_name
    , lag(scans_on_day) over(partition by host_name, schema_name,table_name order by host_name, schema_name,table_name, day) as scans1
    , scans_on_day as scans2
    , lag(size_on_day) over(partition by host_name, schema_name,table_name order by host_name, schema_name,table_name, day) as size1
    , size_on_day as size2
    , sum(scans_on_day) over(partition by day) as global_scans_on_day
from (

    select
          tsd_timestamp::date as day
        , host_name
        , host_id
        , t_schema as schema_name
        , t_name as table_name
        , max(tsd_seq_scans) - min(tsd_seq_scans) as scans_on_day
        , max(tsd_table_size) as size_on_day
       from
            table_size_data
          join tables on t_id = tsd_table_id
          join hosts on host_id = t_host_id
      where tsd_timestamp between $2 - '1d'::interval and $3
        and ( $1 = 'all' or host_name = $1) --like 'customer1-master.db.zalando'
        and t_name not similar to '(temp|_backup)%'
        and t_schema like 'z%'
        and t_schema not similar to '(public|pg_temp|_v|zz_commons|z_sync|temp)%'
        and t_schema not similar to '%_api_r%'
        group by tsd_timestamp::date, host_name, host_id, t_schema, t_name

    ) a

) b
join perf_comparison_default_tables_thresholds on true
left join perf_comparison_thresholds_tables
  on (pctt_table_name, pctt_host_name, pctt_schema_name) = (t_name, host_name, t_schema)
where scans1 is not null
) c
where scan_change_percentage >= allowed_seq_scan_percentage
and size2 >= min_reported_table_size_threshold
and scans2 >= min_reported_scan_count
order by scan_change_percentage desc

$$ language sql;



