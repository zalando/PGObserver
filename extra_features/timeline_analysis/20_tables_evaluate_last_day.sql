
----- now gives real table size, not size change, when p_is_combine_hosts IS FALSE 
CREATE OR REPLACE FUNCTION tables_evaluate_last_day(
  IN  p_date			timestamp without time zone default NULL,
  IN  p_is_combine_hosts   	boolean default 'false',
  IN  p_is_calc_for_size   	boolean default 'true',	-- size OR scans evaluation
  OUT host_name			text array,
  OUT table_name		text, 
  OUT table_size		bigint,
  OUT index_size		bigint,
  OUT table_scans		bigint,
  OUT index_scans		bigint,
  OUT percent_increase		integer
)
  RETURNS setof record AS
$$
DECLARE
  l_number_of_weeks	integer;
  l_alert_percent	integer;
  l_size_threshod	integer;
  l_scan_threshod	integer;
  i			integer;
  l_is_last_day		boolean;
  l_date		timestamp without time zone;
  l_dates_array		timestamp without time zone array;

BEGIN

  l_is_last_day := p_date IS NULL;

  -- fix hour & date
  if (p_date IS NULL) then
     p_date := current_date - interval '1 day';
  else
     p_date := p_date::date;
  end if;


     -- make sure data for all needed periods is there
  select mc_config_value::integer
    from monitoring_configuration
   where mc_config_name = 'size_same_days_past_samples'
    into l_number_of_weeks;

  select mc_config_value::integer
    from monitoring_configuration
   where mc_config_name = 'size_same_days_percent'
    into l_alert_percent;

  select mc_config_value::integer
    from monitoring_configuration
   where mc_config_name = 'size_same_days_threshold'
    into l_size_threshod;

  select mc_config_value::integer
    from monitoring_configuration
   where mc_config_name = 'scans_same_days_threshold'
    into l_scan_threshod;


  i := 0;
  l_date := p_date;
  while i <= l_number_of_weeks loop
    perform calc_tables_summary (l_date);

--raise notice '%',l_date;

    if (i > 0) then 
      l_dates_array := l_dates_array || l_date;
    end if;

    l_date := l_date - interval '7 day';
    i := i+1;
  end loop;


  drop table if exists tmp_prev_sums;
  create temporary table tmp_prev_sums (
    ts_host_id		integer array,
    ts_table_name	text,
    weeks_cnt		integer,	-- number of weeks for which we have data from the sproc !
    ts_table_size 	bigint,
    ts_index_size 	bigint,
    ts_seq_scans	bigint,
    ts_index_scans	bigint
  );

  if (p_is_combine_hosts) then
	  insert into tmp_prev_sums
	  select array_agg(distinct ts_host_id), ts_table_name, count(distinct ts_date) as weeks_cnt, 
		sum(ts_table_size),
		sum(ts_index_size),
		sum(ts_seq_scans),
		sum(ts_index_scans)
	    from tables_summary
	   where ts_date = ANY (l_dates_array) and
		 not exists (select 1 
			       from performance_ignore_list 
			      where (pil_host_id IS NULL or pil_host_id = ts_host_id) AND 
			            (pil_object_name IS NULL or pil_object_name = ts_table_name)
			     )
	  group by ts_table_name;
  else
	  insert into tmp_prev_sums
	  select array_agg(distinct ts_host_id), ts_table_name, count(distinct ts_date) as weeks_cnt, 
		sum(ts_table_size),
		sum(ts_index_size),
		sum(ts_seq_scans),
		sum(ts_index_scans)
	    from tables_summary
	   where ts_date = ANY (l_dates_array) 
	  group by ts_host_id, ts_table_name;
  end if;


  -- average results over non-zero values
  if (p_is_combine_hosts) then

	  RETURN QUERY
	  select array_agg(distinct hosts.host_name), ts.ts_table_name, 
		 sum(ts.ts_table_size)::bigint, sum(ts.ts_index_size)::bigint, 
		 sum(ts.ts_seq_scans)::bigint, sum(ts.ts_index_scans)::bigint,
	         case  when (p_is_calc_for_size) then
			(((sum(1.0*ts.ts_table_size) /  (sum(1.0*tmp.ts_table_size/tmp.weeks_cnt))) - 1.0) * 100.0)::integer
		 else
			(((sum(1.0*ts.ts_seq_scans) /  (sum(1.0*tmp.ts_seq_scans/tmp.weeks_cnt))) - 1.0) * 100.0)::integer
		 end
	    from tables_summary ts
	   inner join tmp_prev_sums tmp
	      on tmp.ts_table_name = ts.ts_table_name
	   inner join hosts
	      on hosts.host_id = ts.ts_host_id
	   where ts.ts_date = p_date::date and
   	         case  when (p_is_calc_for_size) then
			tmp.ts_table_size > 0 
		 else
			tmp.ts_seq_scans > 0 
		 end and
		 tmp.weeks_cnt > 0 and 
		 not exists (select 1 
			       from performance_ignore_list 
			      where (pil_host_id IS NULL or pil_host_id = hosts.host_id) AND 
			            (pil_object_name IS NULL or pil_object_name = ts.ts_table_name)
			     )
-- no filtering if combined hosts   	      and   ( not l_is_last_day OR is_to_be_reported (NULL,'table',ts.ts_table_name,ts.ts_seq_scans) ) -- check reporting threshold only on current day report
	   group by ts.ts_table_name
	   having sum(ts.ts_orig_table_size) > l_size_threshod and -- always filter by size
		  case  when (not p_is_calc_for_size) then
			sum(ts.ts_seq_scans) > l_scan_threshod 
		  end and
   	         case  when (p_is_calc_for_size) then
			sum(1.0*tmp.ts_table_size/tmp.weeks_cnt) * (1.0+1.0*l_alert_percent/100.0) < sum(ts.ts_table_size)
		 else
			sum(1.0*tmp.ts_seq_scans/tmp.weeks_cnt) * (1.0+1.0*l_alert_percent/100.0) < sum(ts.ts_seq_scans)
		 end
	   order by 7 desc;

  else  -- do not combine hosts
	  RETURN QUERY		-- just one row, group by for syntax reasons
	  select array_agg(distinct hosts.host_name), ts.ts_table_name, 
		 max(ts.ts_orig_table_size), max(ts.ts_orig_index_size),
		 max(ts.ts_seq_scans), max(ts.ts_index_scans),
	         case  when (p_is_calc_for_size) then
			max(((((1.0*ts.ts_table_size) / (1.0*tmp.ts_table_size/tmp.weeks_cnt)) -1.0) * 100.0))::integer
		 else
			max(((((1.0*ts.ts_seq_scans) / (1.0*tmp.ts_seq_scans/tmp.weeks_cnt)) -1.0) * 100.0))::integer 
		 end
	    from tables_summary ts
	   inner join tmp_prev_sums tmp
	      on ts.ts_host_id = ALL(tmp.ts_host_id)  and
		 tmp.ts_table_name = ts.ts_table_name
	   inner join hosts
	      on hosts.host_id = ts.ts_host_id
	   where ts.ts_date = p_date::date and
		 ts.ts_orig_table_size > l_size_threshod and -- always filter by size
	         case  when (not p_is_calc_for_size) then
			ts.ts_seq_scans > l_scan_threshod 
		 end and
	         case  when (p_is_calc_for_size) then
			1.0*tmp.ts_table_size/tmp.weeks_cnt * (1.0+1.0*l_alert_percent/100.0) < ts.ts_table_size
		 else
			1.0*tmp.ts_seq_scans/tmp.weeks_cnt * (1.0+1.0*l_alert_percent/100.0) < ts.ts_seq_scans
		 end and
   	         case  when (p_is_calc_for_size) then
			tmp.ts_table_size > 0 
		 else
			tmp.ts_seq_scans > 0 
		 end and
		 tmp.weeks_cnt > 0 and
		 not exists (select 1 
			       from performance_ignore_list 
			      where (pil_host_id IS NULL or pil_host_id = hosts.host_id) AND 
			            (pil_object_name IS NULL or pil_object_name = ts.ts_table_name)
			     ) and
   	         ( not l_is_last_day OR is_to_be_reported (ts.ts_host_id,'table',ts.ts_table_name,ts.ts_seq_scans) ) -- check reporting threshold only on current day report
	   group by hosts.host_name,ts.ts_table_name
	   order by 7 desc; 
  end if;	   
	   
           
END;
$$
  LANGUAGE 'plpgsql';

