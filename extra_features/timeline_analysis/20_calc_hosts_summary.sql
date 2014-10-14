RESET role;

SET search_path TO monitor_data, public;

SET role TO pgobserver_gatherer;

-- select * from calc_hosts_summary ('2014-04-11');
-- select * from hosts_summary where hs_date = '2014-04-11';
CREATE OR REPLACE FUNCTION calc_hosts_summary(
 p_date	timestamp without time zone
)
  RETURNS VOID AS
$$
DECLARE
  l_is_final		boolean;	-- implies that we are caclculting before the day is over, and data should be replaced
  l_calc_for_date	timestamp without time zone;
  l_is_final_results_exist boolean;

BEGIN

  l_calc_for_date := p_date::date + interval '1 day'; -- so we get data for the date requested, not previous day...
  if current_timestamp > l_calc_for_date 
     then l_is_final := 'true';
     else l_is_final := 'false';
  end if;

  -- exit fast if data is 'final' !!

 select 'true'
   from hosts_summary 
  where hs_date = p_date
    and hs_is_final
   into l_is_final_results_exist;

  if (l_is_final_results_exist) then
    RETURN;
  end if;


   drop table if exists tmp_hosts_tables_summary;
   create temporary table tmp_hosts_tables_summary (
     hs_table_id	bigint,
     max_tio_timestamp	timestamp without time zone
   );

  insert into tmp_hosts_tables_summary
  select tii.tio_table_id, max(tii.tio_timestamp)
    from table_io_data tii
   where tii.tio_timestamp >= l_calc_for_date - interval '1 day' and
         tii.tio_timestamp < l_calc_for_date
   group by tii.tio_table_id;

  insert into tmp_hosts_tables_summary
  select tii.tio_table_id, max(tii.tio_timestamp)
    from table_io_data tii
   where tii.tio_timestamp >= l_calc_for_date - interval '2 day' and
         tii.tio_timestamp < l_calc_for_date - interval '1 day'
   group by tii.tio_table_id;


  drop table if exists tmp_hosts_summary;
  create temporary table tmp_hosts_summary (
    hs_host_id		integer,
    hs_cache_misses	bigint,
    hs_date		timestamp without time zone
  );


  insert into tmp_hosts_summary
  select hs_host_id, hs_cache_misses, hs_date
    from
       (

        select 
		tables.t_host_id as hs_host_id, 
		sum ( case when (tid.tio_heap_read + tid.tio_idx_read) > 0 then
				(tid.tio_heap_read + tid.tio_idx_read)
		      else
				0
		      end ) as hs_cache_misses,
		max (tid.tio_timestamp) as hs_date
          from table_io_data tid
         inner join tables
            on tables.t_id = tid.tio_table_id
         inner join tmp_hosts_tables_summary
            on tmp_hosts_tables_summary.hs_table_id = tid.tio_table_id 
           and tmp_hosts_tables_summary.max_tio_timestamp = tid.tio_timestamp
	  where
		tid.tio_timestamp >= l_calc_for_date - interval '1 day' and
		tid.tio_timestamp < l_calc_for_date 
         group by tables.t_host_id

      union -- get previous day too, for delta calculations !!!
        
        select 
		tables.t_host_id as hs_host_id, 
		sum ( case when (tid.tio_heap_read + tid.tio_idx_read) > 0 then 
				(tid.tio_heap_read + tid.tio_idx_read)
		      else
				0
		      end ) as hs_cache_misses,
		max(tid.tio_timestamp) as hs_date
          from table_io_data tid
         inner join tables
            on tables.t_id = tid.tio_table_id
         inner join tmp_hosts_tables_summary
            on tmp_hosts_tables_summary.hs_table_id = tid.tio_table_id 
           and tmp_hosts_tables_summary.max_tio_timestamp = tid.tio_timestamp
	  where
		tid.tio_timestamp >= l_calc_for_date - interval '2 day' and
		tid.tio_timestamp < l_calc_for_date - interval '1 day' 
         group by tables.t_host_id

     ) t;


  drop table if exists tmp_hosts_summary2;
  create temporary table tmp_hosts_summary2 (
    hs_host_id		integer,
    hs_cache_misses	bigint,
    hs_total_cache_misses bigint
  );

  insert into tmp_hosts_summary2
  select coalesce(hc_one_host_id,curr.hs_host_id), 
	sum ( case when (curr.hs_cache_misses - prev.hs_cache_misses) > 0 then
		(curr.hs_cache_misses - prev.hs_cache_misses)
	      else
		curr.hs_cache_misses
	      end ),
	 null
    from tmp_hosts_summary curr
   inner join tmp_hosts_summary prev
      on prev.hs_host_id = curr.hs_host_id
   left join host_clusters 
     on host_clusters.hc_host_id = curr.hs_host_id
   where curr.hs_date::date = l_calc_for_date - interval '1 day' and	-- -1day as we actually have date from 'before' the date...
         prev.hs_date < l_calc_for_date - interval '1 day' 
   group by coalesce(hc_one_host_id,curr.hs_host_id);


  delete from hosts_summary 
  where exists 
        ( select 1 
           from tmp_hosts_summary2 
          where tmp_hosts_summary2.hs_host_id = hosts_summary.hs_host_id
        ) and
        hosts_summary.hs_date = l_calc_for_date - interval '1 day' and
        hosts_summary.hs_is_final = 'false';

    
  insert into hosts_summary (
    hs_host_id,		
    hs_host_name,
    hs_date,		
    hs_cache_misses,	
    hs_total_cache_misses,	
    hs_is_final
  )	
  select 
	hs_host_id,
	host_name,
 	l_calc_for_date - interval '1 day',
        hs_cache_misses,
        hs_total_cache_misses,
	l_is_final
   from tmp_hosts_summary2
  inner join hosts
     on hosts.host_id = tmp_hosts_summary2.hs_host_id;
    
  
END;
$$
  LANGUAGE 'plpgsql';
