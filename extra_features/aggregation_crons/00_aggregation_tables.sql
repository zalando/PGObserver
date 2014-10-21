/*

NB! feature make only sense when you have a lot of monitored servers or a slow DB and the idea was to circumvent hot analytical queries

needed if "run_aggregations" is set to "true" in .pgobserver.conf. also two hourly cronjobs (the two *.py files) need to be set up then

*/


SET ROLE TO pgobserver_gatherer;
SET search_path = monitor_data, public;


--host DB size + IUD counters aggregation. needs an hourly cronjob on aggregate_table_size_data()!
create table table_size_data_agg(
    tsda_timestamp timestamp without time zone NOT NULL,
    tsda_host_id int not null,
    tsda_db_size bigint not null, --table+index
    tsda_tup_ins bigint not null,
    tsda_tup_upd bigint not null,
    tsda_tup_del bigint not null
);
create unique index on table_size_data_agg (tsda_host_id, tsda_timestamp);
create index on table_size_data_agg (tsda_timestamp);


create table sproc_load_agg(
    sla_timestamp timestamp without time zone NOT NULL,
    sla_host_id int not null,
    sla_load_15min numeric(4,2) not null
);
create unique index on sproc_load_agg (sla_host_id, sla_timestamp);
create index on sproc_load_agg (sla_timestamp);



DO $$
DECLARE
  l_tables text[] := array['table_size_data_agg', 'sproc_load_agg'];
  l_table_prefixes text[] := array['tsda', 'sla'];
BEGIN
  FOR i IN 1.. array_upper(l_tables, 1)
  LOOP
    RAISE WARNING 'dropping triggers for: %', l_tables[i];
    EXECUTE 'DROP TRIGGER IF EXISTS ' || l_tables[i] || '_insert_trigger ON ' || l_tables[i];
    RAISE WARNING 'creating triggers for: %', l_tables[i];
    PERFORM monitor_data.create_partitioning_trigger_for_table(l_tables[i], l_table_prefixes[i]);
  END LOOP;
END;
$$
LANGUAGE plpgsql;



-- procs for hourly cronjobs

drop function if exists aggregate_table_size_data(interval,int[]);

create or replace function aggregate_table_size_data(p_timeframe interval, p_host_ids int[] default null)
returns void as
$$
declare
  l_host_ids int[];
  l_current_host_id int;
  l_last_agg_time timestamp;
begin
  if p_host_ids is not null then
    l_host_ids = p_host_ids;
  else
    select array_agg(host_id) into l_host_ids
    from monitor_data.hosts
    where host_enabled
    order by host_id;
  end if;

  foreach l_current_host_id in array l_host_ids
  loop
    --raise warning 'aggregating host_id : %', l_current_host_id;

    select max(tsda_timestamp) into l_last_agg_time from monitor_data.table_size_data_agg where tsda_host_id = l_current_host_id;

    INSERT INTO monitor_data.table_size_data_agg
    SELECT
     tsd_timestamp,
     l_current_host_id as tsd_host_id,
     COALESCE(SUM(tsd_table_size)+SUM(tsd_index_size), 0) AS size,
     COALESCE(SUM(tsd_tup_ins), 0) AS s_ins,
     COALESCE(SUM(tsd_tup_upd), 0) AS s_upd,
     COALESCE(SUM(tsd_tup_del), 0) AS s_del
    FROM
      monitor_data.table_size_data t
    WHERE
      tsd_timestamp > coalesce(l_last_agg_time, now() - p_timeframe)
      --keeping 1h lag for safety. for dbs with tons of tables the actual insertion can take a while and we might get an incomplete picture
      AND tsd_timestamp < now() - '1 hour'::interval
      AND tsd_host_id = l_current_host_id
    GROUP BY
      tsd_timestamp
    ORDER BY
      tsd_timestamp;

  end loop;
end;
$$ language plpgsql security definer;

grant execute on function aggregate_table_size_data(interval,int[]) to public;



drop function if exists aggregate_sproc_load(interval, int[]);
create or replace function aggregate_sproc_load(p_timeframe interval, p_host_ids int[] default null)
returns void as
$$
declare
  l_host_ids int[];
  l_current_host_id int;
  l_last_agg_time timestamp;
begin
  if p_host_ids is not null then
    l_host_ids = p_host_ids;
  else
    select array_agg(host_id) into l_host_ids
    from monitor_data.hosts
    where host_enabled
    order by host_id;
  end if;

  foreach l_current_host_id in array l_host_ids
  loop
    --raise warning 'aggregating host_id : %', l_current_host_id;

    select max(sla_timestamp) into l_last_agg_time from monitor_data.sproc_load_agg where sla_host_id = l_current_host_id;

    INSERT INTO monitor_data.sproc_load_agg
    select
    xaxis,
    l_current_host_id,
    sum(d_self_time) OVER (ORDER BY xaxis ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) / (1*15*60*1000) AS load_15min
   from
     ( SELECT
         date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval AS xaxis,
         sum(t.delta_self_time) AS d_self_time
       FROM ( SELECT
                spd.sp_timestamp,
                COALESCE(spd.sp_self_time - lag(spd.sp_self_time) OVER w, 0::bigint) AS delta_self_time
              FROM
                monitor_data.sproc_performance_data spd
              WHERE
                spd.sp_host_id = l_current_host_id
                AND sp_timestamp > case when l_last_agg_time is not null then l_last_agg_time - '1 hour'::interval else now() - p_timeframe end
                AND sp_timestamp < now() - '1 hour'::interval
              WINDOW w AS
                ( PARTITION BY spd.sp_sproc_id ORDER BY spd.sp_timestamp )

            ) t
       GROUP BY
         date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval
       ORDER BY
         date_trunc('hour'::text, t.sp_timestamp) + floor(date_part('minute'::text, t.sp_timestamp) / 15::double precision) * '00:15:00'::interval
     ) loadTable
     where xaxis > coalesce(l_last_agg_time, now() - p_timeframe);

  end loop;
exception
  when others then
    raise warning 'error when aggregating host_id : %', l_current_host_id;
    raise warning '%', SQLERROR;
end;
$$ language plpgsql security definer;

grant execute on function aggregate_sproc_load(interval,int[]) to public;

