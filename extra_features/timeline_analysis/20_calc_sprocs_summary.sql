-- calculates the sproc run time stats for the past hour (by comaring current total run-time to last hour's total run-time)

CREATE OR REPLACE FUNCTION calc_sprocs_summary(
 p_date	timestamp without time zone,
 p_hour integer	-- note 0==midnight to 1AM...
)
  RETURNS VOID AS
$$
DECLARE
  l_is_final		boolean;	-- implies that we are caclculting before the hour is over, and data should be replaced
  l_calc_for_date	timestamp;
  l_is_final_results_exist boolean;

BEGIN

  l_calc_for_date := p_date::date + p_hour * '1 hour'::interval;
  l_calc_for_date := l_calc_for_date + interval '1 hour';
  if current_timestamp > l_calc_for_date 
     then l_is_final := 'true';
     else l_is_final := 'false';
  end if;

  -- exit fast if data is 'final' !!

 select 'true'
   from sprocs_summary 
  where ss_date = p_date
    and ss_hour = p_hour
    and ss_is_final = 'true'
   into l_is_final_results_exist;

  if (l_is_final_results_exist) then
    RETURN;
  end if;


  drop table if exists tmp_sprocs_summary;
  create temporary table tmp_sprocs_summary (
    ss_host_id		integer,
    sproc_id		integer,  -- as there can be multiple versions of the sproc in the sampled time !!!
    ss_sproc_name	text,
    ss_date		date,
    ss_hour		smallint,
    max_sp_calls	bigint,
    max_sp_total_time 	bigint
  );

  insert into tmp_sprocs_summary
  select ss_host_id, sproc_id, ss_sproc_name, ss_date, ss_hour, max_sp_calls, max_sp_total_time 
    from
       (
        select 
		N.sproc_host_id as ss_host_id, 
		N.sproc_id,
		substring(I.sproc_name from 0 for position( '(' in I.sproc_name)) as ss_sproc_name,
		p_date::date as ss_date, 
		p_hour as ss_hour, 
		max(sp_calls) as max_sp_calls, 
		max(sp_total_time) as max_sp_total_time
          from sproc_performance_data 
         inner join sprocs N
            on sp_sproc_id = N.sproc_id
         inner join sprocs I
            on N.sproc_host_id = I.sproc_host_id and
               substring(N.sproc_name from 0 for position( '(' in N.sproc_name)) = substring(I.sproc_name from 0 for position( '(' in I.sproc_name))
         where sp_timestamp >= l_calc_for_date - interval '1 hour' 
           and sp_timestamp < l_calc_for_date
         group by N.sproc_host_id, N.sproc_id, substring(I.sproc_name from 0 for position( '(' in I.sproc_name)), date_part ('hour',sp_timestamp)

      union -- get previous hour too, for delta calculations !!!
	 select ss_host_id, 0, ss_sproc_name, ss_date, p_hour -1, ss_orig_calls, ss_orig_total_time
	   from sprocs_summary
	   where ss_date = case when (p_hour = 0) then p_date::date - interval '1 day' else p_date::date end
		and ss_hour = case when (p_hour = 0) then 23 else p_hour -1 end

     ) t;

  create index sprocs_idx on tmp_sprocs_summary (ss_host_id,ss_sproc_name,ss_hour);

  drop table if exists tmp_sprocs_summary2;
  create temporary table tmp_sprocs_summary2 (
    ss_host_id		integer,
    ss_sproc_name	text,
    ss_date		date,
    ss_hour		smallint,
    max_sp_calls	bigint,
    max_sp_total_time 	bigint
  );


  insert into tmp_sprocs_summary2
  select 	ss_host_id, 
		ss_sproc_name,
		ss_date,
		ss_hour,
		sum(max_sp_calls),
		sum(max_sp_total_time)
  from tmp_sprocs_summary
  group by ss_host_id, 	ss_sproc_name,	ss_date, ss_hour;

  create index idx2 on tmp_sprocs_summary2 (ss_host_id,ss_sproc_name,ss_hour);

  delete from sprocs_summary 
  where exists 
        ( select 1 
           from tmp_sprocs_summary2 
          where tmp_sprocs_summary2.ss_host_id = sprocs_summary.ss_host_id
            and tmp_sprocs_summary2.ss_sproc_name = sprocs_summary.ss_sproc_name
            and tmp_sprocs_summary2.ss_date = p_date::date
            and tmp_sprocs_summary2.ss_hour = p_hour
            and sprocs_summary.ss_is_final = 'false');
    
  insert into sprocs_summary (
	ss_host_id, 
	ss_sproc_name, 
	ss_date,
	ss_hour, 
	ss_calls,
	ss_total_time,
	ss_orig_calls,
	ss_orig_total_time,
	ss_is_final,
	ss_is_suspect
  )
  select 
	curr.ss_host_id, 
	curr.ss_sproc_name, 
	curr.ss_date,
	curr.ss_hour, 
	case when (curr.max_sp_calls - coalesce(prev.max_sp_calls,0) < 0) then curr.max_sp_calls		-- <0 can happen when proc is in a new schema
	     else curr.max_sp_calls - coalesce(prev.max_sp_calls,0)
	end as ss_calls,
	case when (curr.max_sp_total_time - coalesce(prev.max_sp_total_time,0) < 0) then curr.max_sp_total_time
	     else curr.max_sp_total_time - coalesce(prev.max_sp_total_time,0)
	end as ss_total_time,
	curr.max_sp_calls as ss_orig_calls,
	curr.max_sp_total_time as ss_orig_total_time,
	l_is_final,
	case when (prev.max_sp_calls is null) then true else false end -- handle suspect data
   from tmp_sprocs_summary2 curr
   left join tmp_sprocs_summary2 prev 
     on curr.ss_host_id = prev.ss_host_id and
        curr.ss_sproc_name = prev.ss_sproc_name and
        curr.ss_hour = prev.ss_hour+1
   where curr.ss_hour = p_hour
     and not exists 
         (select 1 
            from sprocs_summary 
           where sprocs_summary.ss_host_id = curr.ss_host_id 
             and sprocs_summary.ss_sproc_name = curr.ss_sproc_name
             and sprocs_summary.ss_date = curr.ss_date
             and sprocs_summary.ss_hour = curr.ss_hour
	     and sprocs_summary.ss_is_final
         );

END;
$$
  LANGUAGE 'plpgsql';

