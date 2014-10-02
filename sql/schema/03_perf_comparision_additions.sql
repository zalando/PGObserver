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
pcst_host_name text primary key,
pcst_sproc_name text,
pcst_allowed_runtime_growth_pct numeric,
pcst_allowed_call_count_growth_pct numeric
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
pctt_host_name text,
pctt_schema_name text,
pctt_table_name text,
pctt_allowed_seq_scan_count bigint,
pctt_allowed_seq_scan_pct numeric,
pctt_allowed_size_growth bigint, --bytes
pctt_allowed_size_growth_pct numeric,
primary key (pctt_host_name, pctt_schema_name, pctt_table_name)
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



/*

sprocs api

*/

--select * from get_sproc_threshold_sinners_for_release('all','r13_00_33','r13_00_34');
drop function if exists get_sproc_threshold_sinners_for_release(text,text,text);

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
, split_part(sproc_name, '(', 1) as sproc_name --drops parameters listing from scproc name
, calltime_change_pct
, share_on_total_runtime
, exec_avg1
, exec_avg2
, calls1
, calls2
, callscount_change_pct
, allowed_runtime_growth_pct
, allowed_share_on_total_runtime_pct
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
        , coalesce (pcst_allowed_runtime_growth_pct, pcdst_allowed_runtime_growth_pct) as allowed_runtime_growth_pct
        , coalesce (pcst_allowed_call_count_growth_pct, pcst_allowed_call_count_growth_pct) as allowed_calls_growth_pct
        , global_total_time
        , pcdst_share_on_total_runtime_pct as allowed_share_on_total_runtime_pct
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
            and host_enabled
            and sproc_host_id = host_id
            and sp_timestamp >= (select min(sproc_created) from sprocs where sproc_schema like '%'|| $2)
         group by host_name, host_id, sproc_schema, sproc_name
       ) a
     ) b
     join perf_comparison_default_sproc_thresholds on true -- 1 row
     left join perf_comparison_sproc_thresholds on (pcst_host_name, pcst_sproc_name) = (host_name, sproc_name)  --custom thresholds
     where ($1 = 'all' or b.host_name = $1)
     and exec_avg1 is not null
) c
where ( calltime_change_pct >= allowed_runtime_growth_pct
    or callscount_change_pct >= allowed_calls_growth_pct)
and share_on_total_runtime >= allowed_share_on_total_runtime_pct
order by 5 desc, 2

$$ language sql set work_mem = '256MB';

grant execute on function get_sproc_threshold_sinners_for_release(text,text,text) to public;







/*

tables api

*/


--select * from get_table_threshold_sinners_for_period('all','2013-08-26','2013-08-28');
drop function if exists get_table_threshold_sinners_for_period(text,date,date);

create or replace function get_table_threshold_sinners_for_period(
          in p_host_name text
        , in p_date1 date
        , in p_date2 date
        , out host_id int
        , out host_name text
        , out schema_name text
        , out table_name text
        , out day date
        , out scan_change_pct numeric
        , out scans1 bigint
        , out scans2 bigint
        , out size1 text
        , out size2 text
        , out size_change_pct numeric
        , out allowed_seq_scan_pct numeric
)
returns setof record
as $$

select
      host_id
    , host_name
    , schema_name
    , table_name
    , day
    , scan_change_pct
    , scans1
    , scans2
    , pg_size_pretty(size1) as size1
    , pg_size_pretty(size2) as size2
    , size_change_pct
    , allowed_seq_scan_pct
from (
    select
          host_name
        , schema_name
        , host_id
        , table_name
        , day
        , scans1
        , scans2
        , case when scans1 = 0 then 0 else round((scans2-scans1)/scans1::numeric*100,2) end as scan_change_pct
        , size1
        , size2
        , case when size1 = 0 then 0 else round((size2-size1)/size1::numeric*100,2) end as size_change_pct
        , coalesce(pctt_allowed_seq_scan_pct,pcdtt_allowed_seq_scan_pct) as allowed_seq_scan_pct
        , coalesce(pctt_allowed_size_growth_pct, pcdtt_allowed_size_growth_pct) as allowed_size_growth_pct
        , coalesce(pctt_allowed_seq_scan_count,pcdtt_min_reported_scan_count) as min_reported_scan_count    
        , pcdtt_min_reported_table_size_threshold as min_reported_table_size_threshold
        , coalesce(pctt_allowed_size_growth,pcdtt_allowed_size_growth) as allowed_size_growth
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
           from table_size_data
              join tables on t_id = tsd_table_id
              join hosts on host_id = t_host_id
          where tsd_timestamp between $2 - '1d'::interval and $3
            and ( $1 = 'all' or host_name = $1)
            and ( $1 = 'all' or tsd_host_id = (select host_id from hosts where host_name = $1 and host_enabled))
            and host_enabled
            and t_name not similar to '(temp|_backup)%'
            and t_schema like 'z%'
            and t_schema not like any(select pcsi_schema from perf_comparison_schemas_ignored)
            and t_schema||t_name not in (select pcti_schema||pcti_table from perf_comparison_tables_ignored)
            group by tsd_timestamp::date, host_name, host_id, t_schema, t_name

        ) a
    ) b
    join perf_comparison_default_tables_thresholds on true
    left join perf_comparison_table_thresholds
      on (pctt_table_name, pctt_host_name, pctt_schema_name) = (table_name, host_name, schema_name)
    where scans1 is not null
) c
where size2 >= min_reported_table_size_threshold
and scans2 >= min_reported_scan_count
and (
     scan_change_pct >= allowed_seq_scan_pct
  or size_change_pct >= allowed_size_growth_pct
  or (size2 - size1) > allowed_size_growth
)
order by scan_change_pct desc

$$ language sql;

grant execute on function get_table_threshold_sinners_for_period(text,date,date) to public;
