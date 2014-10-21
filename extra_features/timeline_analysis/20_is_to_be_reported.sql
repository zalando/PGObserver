-- this function was added to filter noisy reports regarding sprocs performance checks - it reports just sprocs that reached a new high 
-- (or 2nd) record high compared to the past few days (configrable)


CREATE OR REPLACE FUNCTION is_to_be_reported (  -- evaluated only ONE statistic per object, to decide if fluction needs to be reported
  IN  p_host_id			integer,
  IN  p_object_type		text, -- sproc/table/cache
  IN  p_object_name		text,
  IN  p_detected_value		bigint
)
  RETURNS boolean AS
$$
DECLARE
  l_max_higher_values	integer;
  l_past_days_to_check	integer;
  l_cnt			integer;

BEGIN
  -- get run configuration

  select mc_config_value::integer
    from monitoring_configuration
   where mc_config_name = 'report_max_higher_values'
    into l_max_higher_values;


  select mc_config_value::integer
    from monitoring_configuration
   where mc_config_name = 'report_past_days_to_check'
    into l_past_days_to_check;

  -- sproc
  if p_object_type = 'sproc' then
     select count(1) 
       from sprocs_summary
      where ss_date > current_timestamp - l_past_days_to_check * '1 day'::interval and
            ss_total_time > p_detected_value and
            ss_sproc_name = p_object_name and
            ss_host_id = coalesce (p_host_id,ss_host_id)
       into l_cnt;

     return l_cnt < l_max_higher_values;



  -- table
  else if p_object_type = 'table' then

     select count(1) 
       from tables_summary
      where ts_date > current_timestamp - l_past_days_to_check * '1 day'::interval and
            ts_table_size > p_detected_value and
            ts_table_name = p_object_name and
            ts_host_id = coalesce (p_host_id,ts_host_id)
       into l_cnt;

     return l_cnt < l_max_higher_values;


  -- cache
  else if p_object_type = 'cache' then

     select count(1) 
       from hosts_summary
      where hs_date > current_timestamp - l_past_days_to_check * '1 day'::interval and
            hs_cache_misses > p_detected_value and
            hs_host_id = p_host_id
       into l_cnt;

     return l_cnt < l_max_higher_values;

  end if;
  end if;
  end if;

END;
$$
  LANGUAGE 'plpgsql';
