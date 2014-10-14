CREATE OR REPLACE FUNCTION hosts_evaluate_performance_last_day(
  IN  p_date			timestamp without time zone default NULL,
  OUT host_name			text array,
  OUT cache_misses		bigint,
  OUT time_percent_increase	integer
)
  RETURNS setof record AS
$$
DECLARE
  l_number_of_weeks	integer;
  l_alert_percent	integer;
  l_misses_threshod	integer;
  l_dates_array		timestamp without time zone array;
  i			integer;
  l_date		timestamp without time zone;
  
BEGIN

  -- fix hour & date
  if (p_date IS NULL) then
      p_date := current_date::date - interval '1 day';
  else
      p_date := p_date::date;
  end if;

   
  -- make sure data for all needed periods is there
  select mc_config_value::integer
    from monitoring_configuration
   where mc_config_name = 'total_hosts_same_days_past_samples'
    into l_number_of_weeks;

  select mc_config_value::integer
    from monitoring_configuration
   where mc_config_name = 'total_hosts_same_days_percent'
    into l_alert_percent;

  select mc_config_value::integer
    from monitoring_configuration
   where mc_config_name = 'total_hosts_same_days_threshold'
    into l_misses_threshod;


  i := 0;
  l_date := p_date;
  while i <= l_number_of_weeks loop
    perform calc_hosts_summary (l_date);

--raise notice '%',l_date;

    if (i > 0) then 
      l_dates_array := l_dates_array || l_date;
    end if;

    l_date := l_date - interval '7 day';
    i := i+1;
  end loop;


  drop table if exists tmp_prev_sums;
  create temporary table tmp_prev_sums (
    hs_host_id		integer,
    hs_host_name	text array,
    weeks_cnt		integer,	-- number of weeks for which we have data from the sproc !
    hs_cache_misses	bigint
  );

  insert into tmp_prev_sums
  select hs_host_id, 
	 case when (max(hc_one_host_id) IS NULL) then
		array [max(hosts_summary.hs_host_name)]
	 else 
		array_agg (distinct hosts.host_name)
	 end,
	 count(hs_date) as weeks_cnt, sum(hs_cache_misses) as hs_cache_misses
    from hosts_summary
    left join host_clusters on
         hc_one_host_id = hosts_summary.hs_host_id
    left join hosts on
	 hosts.host_id = host_clusters.hc_host_id
   where hs_date = ANY (l_dates_array) and
	  hs_cache_misses > l_misses_threshod
  group by hs_host_id, 
	case when (host_clusters.hc_one_host_id is NOT NULL)  then
		host_clusters.hc_one_host_id
	else
		hosts_summary.hs_host_id
	end;


  RETURN QUERY
  select tmp.hs_host_name, hs.hs_cache_misses, ((((1.0*hs.hs_cache_misses)  / (1.0*tmp.hs_cache_misses/tmp.weeks_cnt)) - 1.0) *100.0)::integer
    from hosts_summary hs
   inner join tmp_prev_sums tmp
      on tmp.hs_host_id = hs.hs_host_id
   where hs.hs_date = p_date and
	 tmp.hs_cache_misses > 0 and
	 1.0*tmp.hs_cache_misses/tmp.weeks_cnt * (1.0+1.0*l_alert_percent/100.0) < hs.hs_cache_misses and
	 ( p_date IS NOT NULL OR is_to_be_reported (hs.hs_host_id,'cache',null, hs.hs_cache_misses) ) -- check reporting threshold only on current day report
   order by 3 desc; 
           
END;
$$
  LANGUAGE 'plpgsql';
