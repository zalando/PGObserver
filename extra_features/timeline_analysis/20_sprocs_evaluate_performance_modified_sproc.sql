----select * from sprocs_evaluate_performance_modified_sproc('false','true')
CREATE OR REPLACE FUNCTION sprocs_evaluate_performance_modified_sproc(
  IN  p_is_eval_averages	boolean default 'true',
  IN  p_is_combine_hosts   	boolean default 'true',
  OUT host_name			text ARRAY,
  OUT sproc_name		text, 
  OUT calls			bigint,
  OUT total_time		bigint,
  OUT avg_time			bigint,
  OUT time_percent_increase	integer
)
  RETURNS setof record AS
--returns void as
$$
DECLARE
  l_number_of_weeks	integer;
  l_alert_percent	integer;
  i			integer;
  l_date		timestamp without time zone;
  l_hour		integer;
  l_prev_date		timestamp without time zone;
  l_prev_hour		integer;
  l_date_static		timestamp without time zone;
  l_dates_array_prev	timestamp without time zone array;
  l_dates_array_curr	timestamp without time zone array;
  l_rowcnt		integer;

BEGIN

  -- fix hour & date and defaults
  l_hour := extract (hour from current_time);
  l_date := current_date;
  l_date_static := l_date;
  if (l_hour = 0) then
     l_prev_hour := 23;
     l_prev_date := current_date - interval '1 day';
  else
     l_prev_hour := l_hour -1;   
     l_prev_date := current_date;
  end if;

  -- the procs to be processed
  drop table if exists tmp_changed_sprocs;
  create table tmp_changed_sprocs (
    host_id			integer,
    sproc_name			text,
    create_time			timestamp without time zone,
    min_prev_hour_stats_time	timestamp without time zone, -- first stats collection AFTER the create_time
    max_prev_hour_stats_time	timestamp without time zone, -- last stats collection previous hour
    minutes_prev_hour		integer,
    calls_prev_hour		bigint,
    total_time_prev_hour	bigint,
    min_curr_hour_stats_time	timestamp without time zone, -- current hour
    max_curr_hour_stats_time	timestamp without time zone, -- current hour
    minutes_curr_hour		integer,
    calls_curr_hour		bigint,
    total_time_curr_hour	bigint
  );

  -- first find if any procs need processing
  insert into tmp_changed_sprocs (host_id, sproc_name,	create_time)
  select scd_host_id, scd_sproc_name, max(scd_detection_time)
    from sproc_change_detected
   where scd_detection_time + interval '2 hour' > current_timestamp and
         scd_is_new_hash
   group by scd_host_id, scd_sproc_name;
  GET DIAGNOSTICS l_rowcnt = ROW_COUNT;

  if (l_rowcnt = 0) then
    RETURN;
  end if;

  
     -- make sure data for all needed past periods is there
  select mc_config_value::integer
    from monitoring_configuration
   where mc_config_name = 'total_time_same_days_hourly_past_samples'
    into l_number_of_weeks;

  select mc_config_value::integer
    from monitoring_configuration
   where mc_config_name = 'total_time_same_days_hourly_percent'
    into l_alert_percent;

  i := 1;	-- start from previous week, no current data from sprocs_summary, as we are looking at partial information
  while i <= l_number_of_weeks loop
    l_date := l_date - interval '7 day';
    perform calc_sprocs_summary (l_date, l_hour);
    l_dates_array_prev := l_dates_array_prev || l_date;
    i := i+1;
  end loop;

  i := 1;	-- and for previous hour
  while i <= l_number_of_weeks loop
    l_prev_date := l_prev_date - interval '7 day';
    perform calc_sprocs_summary (l_prev_date, l_prev_hour);
    l_dates_array_curr := l_dates_array_prev || l_prev_date;
    i := i+1;
  end loop;


  -- update the tmp table with the recent data...
  -- prev hour
  update tmp_changed_sprocs set 
	min_prev_hour_stats_time = min_sp_timestamp,
	max_prev_hour_stats_time = max_sp_timestamp
   from ( select N.sproc_host_id as sproc_host_id, 
		 substring(N.sproc_name from 0 for position( '(' in N.sproc_name)) as sproc_name, 
		 min(sp_timestamp) as min_sp_timestamp, max(sp_timestamp) as max_sp_timestamp
	    from sproc_performance_data
	   inner join sprocs N
              on sp_sproc_id = N.sproc_id 
           inner join tmp_changed_sprocs tmp
              on sp_timestamp > tmp.create_time and
		  substring(N.sproc_name from 0 for position( '(' in N.sproc_name)) = tmp.sproc_name and
		  tmp.host_id = N.sproc_host_id
	   where  
		 sp_timestamp < current_date + extract (hour from current_time) * interval '1 hour' 
	   group by N.sproc_host_id, substring(N.sproc_name from 0 for position( '(' in N.sproc_name))
	  having min(sp_timestamp) != max(sp_timestamp) ) t -- not just one sample per hour!
   where t.sproc_host_id = tmp_changed_sprocs.host_id and
	 t.sproc_name = tmp_changed_sprocs.sproc_name and
	 t.min_sp_timestamp > tmp_changed_sprocs.create_time;


  -- curr hour
  update tmp_changed_sprocs set 
	min_curr_hour_stats_time = min_sp_timestamp,
	max_curr_hour_stats_time = max_sp_timestamp
   from ( select N.sproc_host_id as sproc_host_id, 
		 substring(N.sproc_name from 0 for position( '(' in N.sproc_name)) as sproc_name, 
		 min(sp_timestamp) as min_sp_timestamp, max(sp_timestamp) as max_sp_timestamp
	    from sproc_performance_data
	   inner join sprocs N
              on sp_sproc_id = N.sproc_id 
	   where sp_timestamp > current_date + extract (hour from current_timestamp) * interval '1 hour' and 
		 sp_timestamp < current_timestamp 
	   group by N.sproc_host_id, substring(N.sproc_name from 0 for position( '(' in N.sproc_name))
	  having min(sp_timestamp) != max(sp_timestamp) ) t -- not just one sample per hour!
   where t.sproc_host_id = tmp_changed_sprocs.host_id and
	 t.sproc_name = tmp_changed_sprocs.sproc_name;


  update tmp_changed_sprocs set 
	minutes_prev_hour = extract (minute from lst.sp_timestamp - prev.sp_timestamp),
	calls_prev_hour	= lst.sp_calls - prev.sp_calls,
	total_time_prev_hour = lst.sp_total_time - prev.sp_total_time
   from sproc_performance_data lst
  inner join sproc_performance_data prev
     on prev.sp_sproc_id = lst.sp_sproc_id
  inner join sprocs N
     on prev.sp_sproc_id = N.sproc_id
  where prev.sp_timestamp = min_prev_hour_stats_time and
        lst.sp_timestamp = max_prev_hour_stats_time and
        substring(N.sproc_name from 0 for position( '(' in N.sproc_name)) = tmp_changed_sprocs.sproc_name and
        tmp_changed_sprocs.host_id = N.sproc_host_id;


  update tmp_changed_sprocs set 
	minutes_curr_hour = extract (minute from lst.sp_timestamp - prev.sp_timestamp),
	calls_curr_hour	= lst.sp_calls - prev.sp_calls,
	total_time_curr_hour = lst.sp_total_time - prev.sp_total_time
   from sproc_performance_data lst
  inner join sproc_performance_data prev
     on prev.sp_sproc_id = lst.sp_sproc_id
  inner join sprocs N
     on prev.sp_sproc_id = N.sproc_id
  where prev.sp_timestamp = min_curr_hour_stats_time and
        lst.sp_timestamp = max_curr_hour_stats_time and
        substring(N.sproc_name from 0 for position( '(' in N.sproc_name)) = tmp_changed_sprocs.sproc_name and
        tmp_changed_sprocs.host_id = N.sproc_host_id;


  -- now we have per host number with number of minutes per proc name
  -- handle combined hosts
  if (p_is_combine_hosts) then

    drop table if exists tmp_changed_sprocs_combined;
    create table tmp_changed_sprocs_combined (
      sproc_name		text,
      create_time		timestamp,
      minutes_prev_hour		integer,
      calls_prev_hour		bigint,
      total_time_prev_hour	bigint,
      minutes_curr_hour		integer,
      calls_curr_hour		bigint,
      total_time_curr_hour	bigint
    );

    insert into tmp_changed_sprocs_combined
    select tmp_changed_sprocs.sproc_name,
	   max(tmp_changed_sprocs.create_time),
	   sum(tmp_changed_sprocs.minutes_prev_hour),
	   sum(tmp_changed_sprocs.calls_prev_hour),
	   sum(tmp_changed_sprocs.total_time_prev_hour),
	   sum(tmp_changed_sprocs.minutes_curr_hour),
	   sum(tmp_changed_sprocs.calls_curr_hour),
	   sum(tmp_changed_sprocs.total_time_curr_hour)
      from tmp_changed_sprocs
     group by tmp_changed_sprocs.sproc_name;
	  
  end if;


  -- do prev_sums --  note ONE RECORD PER HOUR, unlike tmp_changed_sprocs
  drop table if exists tmp_prev_sums;
  create temporary table tmp_prev_sums (
    ss_host_id		integer array,
    ss_sproc_name	text,
    ss_hour		integer,
    weeks_cnt		integer,
    sum_calls		bigint,
    sum_total_time	bigint
  );

  if (p_is_combine_hosts) then

	  insert into tmp_prev_sums
	  select array_agg(ss_host_id), 
		 ss_sproc_name, 
		 max(ss_hour),
		 count(distinct ss_date) as weeks_cnt, 
		 sum(ss_calls)*60/max(minutes_prev_hour) as sum_calls, 
		 sum(ss_total_time)*60/max(minutes_prev_hour) as sum_total_time
	    from sprocs_summary
	   inner join tmp_changed_sprocs_combined
	      on tmp_changed_sprocs_combined.sproc_name = sprocs_summary.ss_sproc_name --and
	   where ss_hour = l_prev_hour and
		 not ss_is_suspect and
		 ss_date = ANY (l_dates_array_prev) and
		 not exists (select 1 
			       from performance_ignore_list 
			      where (pil_host_id IS NULL or pil_host_id = ss_host_id) AND 
			            (pil_object_name IS NULL or pil_object_name = ss_sproc_name)
			     )
	  group by ss_sproc_name;

	  insert into tmp_prev_sums
	  select array_agg(ss_host_id), 
		 ss_sproc_name, 
		 max(ss_hour),
		 count(distinct ss_date) as weeks_cnt, 
		 sum(ss_calls)*60/max(minutes_curr_hour) as sum_calls, 
		 sum(ss_total_time)*60/max(minutes_curr_hour) as sum_total_time
	    from sprocs_summary
	   inner join tmp_changed_sprocs_combined
	      on tmp_changed_sprocs_combined.sproc_name = sprocs_summary.ss_sproc_name
	   where ss_hour = l_hour and
		 not ss_is_suspect and
		 ss_date = ANY (l_dates_array_curr)
	  group by ss_sproc_name;

  else

	  insert into tmp_prev_sums
	  select array_agg(ss_host_id), 
		 ss_sproc_name, 
		 max(ss_hour),
		 count(distinct ss_date) as weeks_cnt, 
		 sum(ss_calls)*60/max(minutes_prev_hour) as sum_calls, 
		 sum(ss_total_time)*60/max(minutes_prev_hour) as sum_total_time
	    from sprocs_summary
	   inner join tmp_changed_sprocs
	      on tmp_changed_sprocs.sproc_name = sprocs_summary.ss_sproc_name and
		 tmp_changed_sprocs.host_id = sprocs_summary.ss_host_id
	   where ss_hour = l_prev_hour and
		 not ss_is_suspect and
	         ss_date = ANY (l_dates_array_prev)
	  group by ss_host_id, ss_sproc_name;

	  insert into tmp_prev_sums
	  select array_agg(ss_host_id), 
		 ss_sproc_name, 
		 max(ss_hour),
		 count(distinct ss_date) as weeks_cnt, 
		 sum(ss_calls)*60/max(minutes_curr_hour) as sum_calls, 
		 sum(ss_total_time)*60/max(minutes_curr_hour) as sum_total_time
	    from sprocs_summary
	   inner join tmp_changed_sprocs
	      on tmp_changed_sprocs.sproc_name = sprocs_summary.ss_sproc_name and
		 tmp_changed_sprocs.host_id = sprocs_summary.ss_host_id
	   where ss_hour = l_hour and
		 not ss_is_suspect and
	         ss_date = ANY (l_dates_array_curr)
	  group by ss_host_id, ss_sproc_name;
  end if;

   



  -- average results over non-zero values
  if (p_is_combine_hosts) then 	-- must separate, as if I combine hosts, I no longer have a valid host_id tmp_prev_sums
    RETURN QUERY
    select array_agg(hosts.host_name), ss.sproc_name, 
	 sum(coalesce(calls_prev_hour,0) + coalesce(calls_curr_hour,0))::bigint, 
	 sum(coalesce(total_time_prev_hour,0) + coalesce(total_time_curr_hour,0))::bigint,  
	 ( sum(coalesce(total_time_prev_hour,0) + coalesce(total_time_curr_hour,0)) / sum(coalesce(calls_prev_hour,0) + coalesce(calls_curr_hour,0)) )::bigint,

	 case when p_is_eval_averages then
		(( avg( 1.0 * (coalesce(total_time_prev_hour,0) + coalesce(total_time_curr_hour,0)) / (coalesce(calls_prev_hour,0) + coalesce(calls_curr_hour,0)) ) /  
		   avg( 1.0 * (coalesce(prev.sum_total_time,0)+coalesce(curr.sum_total_time,0)) / (coalesce(prev.sum_calls,0)+coalesce(curr.sum_calls,0)) ) 
		-1.0) * 100.0)::integer
	 else	
		(( sum( 1.0 * coalesce(total_time_prev_hour,0) + coalesce(total_time_curr_hour,0) ) /
		   sum( 1.0 * (coalesce(prev.sum_total_time,0)+coalesce(curr.sum_total_time,0)) / curr.weeks_cnt )
		-1.0) * 100.0)::integer
	 end

    from tmp_changed_sprocs_combined ss
    left join tmp_prev_sums curr
      on curr.ss_sproc_name = ss.sproc_name and
         curr.ss_hour = l_hour
    left join tmp_prev_sums prev
      on prev.ss_sproc_name = ss.sproc_name and
         (curr.ss_host_id is null or curr.ss_host_id = prev.ss_host_id) and
	 prev.ss_hour = l_prev_hour
   left join hosts
      on hosts.host_id = ANY (curr.ss_host_id)

   where 
	 coalesce(prev.sum_total_time,0) >= 0 and
	 coalesce(curr.sum_total_time,0) > 0 and
	 coalesce(curr.sum_calls,0) > 0 and
	 case when p_is_eval_averages then
		'true'
	 else	
		( 1.0 * (coalesce(prev.sum_total_time,0)+coalesce(curr.sum_total_time,0)) / curr.weeks_cnt )
			* (1.0+1.0*l_alert_percent/100.0) < 
		(coalesce(total_time_prev_hour,0) + coalesce(total_time_curr_hour,0))
	 end and
	 not exists (select 1 
		       from performance_ignore_list 
		      where (pil_host_id IS NULL or pil_host_id = hosts.host_id) AND 
			     (pil_object_name IS NULL or pil_object_name = ss.sproc_name)
		    )

   group by ss.sproc_name
   having --sum(ss.ss_total_time) > l_time_threshod and
	 sum( 1.0 * (coalesce(prev.sum_total_time,0)+coalesce(curr.sum_total_time,0)) ) > 0 and
	 case when p_is_eval_averages then
		avg( 1.0 * (coalesce(prev.sum_total_time,0) + coalesce(curr.sum_total_time,0)) / (coalesce(prev.sum_calls,0) + coalesce(curr.sum_calls,0)) )  
			* (1.0+1.0*l_alert_percent/100.0) < 
		avg( 1.0*(coalesce(total_time_prev_hour,0) + coalesce(total_time_curr_hour,0)) / (coalesce(calls_prev_hour,0) + coalesce(calls_curr_hour,0)) )
	 else	
		'true'
	 end 
   order by 6 desc; 

  else -- no host combined, still needs to combine the two hours, if any

    RETURN QUERY  
    select array[hosts.host_name], ss.sproc_name, 
	 coalesce(calls_prev_hour,0) + coalesce(calls_curr_hour,0), 
	 coalesce(total_time_prev_hour,0) + coalesce(total_time_curr_hour,0),  
	 ( (coalesce(total_time_prev_hour,0) + coalesce(total_time_curr_hour,0)) / (coalesce(calls_prev_hour,0) + coalesce(calls_curr_hour,0)) )::bigint,
	 case when p_is_eval_averages then
		(( ( 1.0 * (coalesce(total_time_prev_hour,0) + coalesce(total_time_curr_hour,0)) / (coalesce(calls_prev_hour,0) + coalesce(calls_curr_hour,0)) ) /  
		   ( 1.0 * (coalesce(prev.sum_total_time,0)+coalesce(curr.sum_total_time,0)) / (coalesce(prev.sum_calls,0)+coalesce(curr.sum_calls,0)) ) 
		-1.0) * 100.0)::integer
	 else	
		(( ( 1.0 * coalesce(total_time_prev_hour,0) + coalesce(total_time_curr_hour,0) ) /
		   ( 1.0 * (coalesce(prev.sum_total_time,0)+coalesce(curr.sum_total_time,0)) / curr.weeks_cnt )
		-1.0) * 100.0)::integer
	 end

    from tmp_changed_sprocs ss
   inner join hosts
      on hosts.host_id = ss.host_id
    left join tmp_prev_sums prev
      on prev.ss_sproc_name = ss.sproc_name and
         ss.host_id = ALL(prev.ss_host_id) and
	 prev.ss_hour = l_prev_hour
    left join tmp_prev_sums curr
      on curr.ss_sproc_name = ss.sproc_name and
         ss.host_id = ALL(curr.ss_host_id) and
         curr.ss_hour = l_hour

   where
	 coalesce(prev.sum_total_time,0) >= 0 and
	 coalesce(curr.sum_total_time,0) >= 0 and
	 case when p_is_eval_averages then
		( 1.0 * (coalesce(prev.sum_total_time,0) + coalesce(curr.sum_total_time,0)) / (coalesce(prev.sum_calls,0) + coalesce(curr.sum_calls,0)) ) 
			* (1.0+1.0*l_alert_percent/100.0) < 
		( 1.0*(coalesce(total_time_prev_hour,0) + coalesce(total_time_curr_hour,0)) / (coalesce(calls_prev_hour,0) + coalesce(calls_curr_hour,0)) )
	 else	
		( 1.0 * (coalesce(prev.sum_total_time,0)+coalesce(curr.sum_total_time,0)) / curr.weeks_cnt )
			* (1.0+1.0*l_alert_percent/100.0) < 
		(coalesce(total_time_prev_hour,0) + coalesce(total_time_curr_hour,0))
	 end and
	 not exists (select 1 
		       from performance_ignore_list 
		      where (pil_host_id IS NULL or pil_host_id = hosts.host_id) AND 
			     (pil_object_name IS NULL or pil_object_name = ss.sproc_name)
		    )

   order by 6 desc; 

  end if;
           
END;
$$
  LANGUAGE 'plpgsql';
