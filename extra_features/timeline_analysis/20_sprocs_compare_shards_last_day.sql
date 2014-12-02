-- this function compares 'shards' to each other for the past day, this is the best indication that something got wrong in
﻿-- one of the shards, like a missing index (e.g. an index that became invalid during rebuild)
﻿
﻿
﻿CREATE OR REPLACE FUNCTION sprocs_compare_shards_last_day(
  IN  p_date			timestamp without time zone default NULL,
  OUT test_name			text,
  OUT shard1			text,
  OUT shard2			text,
  OUT ratio			integer,
  OUT object_name		text
)
  RETURNS setof record AS
$$
DECLARE
  l_shard_compare_min_total_time integer;
  l_shard_compare_min_tbl_scans integer;
  l_shard_compare_min_factor_to_report integer;
  l_size_threshod integer;
BEGIN


  if (p_date IS NULL) then
     p_date := current_date - interval '1 day';
  else
     p_date := p_date::date;
  end if;

   
  select mc_config_value::integer
    from monitoring_configuration
   where mc_config_name = 'shard_compare_min_total_time'
    into l_shard_compare_min_total_time;

  select mc_config_value::integer
    from monitoring_configuration
   where mc_config_name = 'shard_compare_min_tbl_scans'
    into l_shard_compare_min_tbl_scans;


  select mc_config_value::integer
    from monitoring_configuration
   where mc_config_name = 'shard_compare_min_factor_to_report'
    into l_shard_compare_min_factor_to_report;

  select mc_config_value::integer
    from monitoring_configuration
   where mc_config_name = 'size_same_days_threshold'
    into l_size_threshod;

	  RETURN QUERY
    -- increase in avg time
	select 'avg sproc time'::text test_name, 
		case when ((sum(1.0*a.ss_total_time/a.ss_calls)) / (sum(1.0*b.ss_total_time/b.ss_calls)))::integer > 0
		     then c.host_name 
		     else d.host_name
		end shard1, 
		case when ((sum(1.0*a.ss_total_time/a.ss_calls)) / (sum(1.0*b.ss_total_time/b.ss_calls)))::integer > 0
		     then d.host_name 
		     else c.host_name
		end shard2, 
		CASE when ((sum(1.0*a.ss_total_time/a.ss_calls)) / (sum(1.0*b.ss_total_time/b.ss_calls)))::integer > 0
		       then ((sum(1.0*a.ss_total_time/a.ss_calls)) / (sum(1.0*b.ss_total_time/b.ss_calls)))::integer
		     else ((sum(1.0*b.ss_total_time/b.ss_calls)) / (sum(1.0*a.ss_total_time/a.ss_calls)))::integer
		END AS ratio,
		a.ss_sproc_name object_name
	  from sprocs_summary a
	 inner join sprocs_summary b
	    on a.ss_date = b.ss_date and a.ss_hour = b.ss_hour and a.ss_sproc_name = b.ss_sproc_name
	 inner join "hosts" c
	    on c.host_id = a.ss_host_id
	 inner join "hosts" d
	    on d.host_id = b.ss_host_id
	 inner join shard_pairs
	    on shard_1st_host_id = a.ss_host_id and shard_2nd_host_id = b.ss_host_id
 	 where a.ss_date = p_date and
 	       a.ss_calls > 0 and
 	       b.ss_calls > 0 and
 	       a.ss_total_time > 0 and
 	       b.ss_total_time > 0
      -- do not report known "problem makers"
	  and not exists (select 1 
		       from performance_ignore_list 
		      where (pil_host_id IS NULL or pil_host_id = c.host_id) AND 
			     (pil_object_name IS NULL or pil_object_name = a.ss_sproc_name)
		    ) 
	  and not exists (select 1 
		       from performance_ignore_list 
		      where (pil_host_id IS NULL or pil_host_id = d.host_id) AND 
			     (pil_object_name IS NULL or pil_object_name = a.ss_sproc_name)
		    ) 
	 group by 
		c.host_name, d.host_name, a.ss_date,
		a.ss_sproc_name
	having
	  sum(a.ss_calls) != 0
	  and sum(b.ss_calls) != 0
	  and sum(a.ss_total_time) != 0
	  and sum(b.ss_total_time) != 0
	  and sum(1.0*a.ss_total_time/a.ss_calls) !=0
	  and sum(1.0*b.ss_total_time/b.ss_calls) !=0
	  and (sum(a.ss_total_time) > l_shard_compare_min_total_time OR sum(b.ss_total_time) > l_shard_compare_min_total_time)
	  and ( ((sum(1.0*a.ss_total_time/a.ss_calls)) > (sum(1.0*b.ss_total_time/b.ss_calls)) *l_shard_compare_min_factor_to_report) OR ((sum(1.0*a.ss_total_time/a.ss_calls))*l_shard_compare_min_factor_to_report < (sum(1.0*b.ss_total_time/b.ss_calls))) )

	UNION 

	-- increase in table scans
	select 'seq table scans'::text, 
		CASE when ((1.0*a.ts_seq_scans) / (1.0*b.ts_seq_scans))::integer > 0
		  then c.host_name
		  else d.host_name
		end, 
		CASE when ((1.0*a.ts_seq_scans) / (1.0*b.ts_seq_scans))::integer > 0
		  then d.host_name
		  else c.host_name
		end, 
		CASE when ((1.0*a.ts_seq_scans) / (1.0*b.ts_seq_scans))::integer > 0
		       then ((1.0*a.ts_seq_scans) / (1.0*b.ts_seq_scans))::integer
		     else ((1.0*b.ts_seq_scans) / (1.0*a.ts_seq_scans))::integer
		END,
		a.ts_table_name object_name
	  from tables_summary a
	 inner join tables_summary b
	    on a.ts_date = b.ts_date and a.ts_table_name = b.ts_table_name
	 inner join "hosts" c
	    on c.host_id = a.ts_host_id
	 inner join "hosts" d
	    on d.host_id = b.ts_host_id
	 inner join shard_pairs
	    on shard_1st_host_id = a.ts_host_id and shard_2nd_host_id = b.ts_host_id
	 where ( ((1.0*a.ts_seq_scans) > (1.0*b.ts_seq_scans) *l_shard_compare_min_factor_to_report) OR ((1.0*a.ts_seq_scans)*l_shard_compare_min_factor_to_report < (1.0*b.ts_seq_scans)) )
	   and (a.ts_seq_scans > l_shard_compare_min_tbl_scans or b.ts_seq_scans > l_shard_compare_min_tbl_scans)
	   and a.ts_date = p_date
           and a.ts_orig_table_size > l_size_threshod
	 order by test_name, ratio desc, shard1, shard2, object_name ;

           
END;
$$
  LANGUAGE 'plpgsql';
