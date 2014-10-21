-- calcualtes the number of table-scans and table-size delta for the past day (by subtracting the totals from the prev day)

CREATE OR REPLACE FUNCTION calc_tables_summary(
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
   from tables_summary 
  where ts_date = p_date
    and ts_is_final
   into l_is_final_results_exist;

  if (l_is_final_results_exist) then
    RETURN;
  end if;


  drop table if exists tmp_tables_summary;
  create temporary table tmp_tables_summary (
    ts_host_id		integer,
    ts_table_id		integer,
    ts_table_name	text,
    ts_date		date,
    ts_table_size	bigint, 
    ts_index_size	bigint,
    ts_seq_scans	bigint,
    ts_index_scans	bigint
  );

  insert into tmp_tables_summary
  select t_host_id, t_id, t_name, ts_date, ts_table_size, ts_index_size, ts_seq_scans, ts_index_scans
    from
       (
        select 
		t_host_id, 
		t_id,
		t_name,
		max(tsd_timestamp) as ts_date, 
		max(tsd_table_size) as ts_table_size, 
		max(tsd_index_size) as ts_index_size,
		max(tsd_seq_scans) as ts_seq_scans, 
		max(tsd_index_scans) as ts_index_scans
          from table_size_data 
         inner join "tables" 
            on tsd_table_id = t_id
         where tsd_timestamp >= l_calc_for_date - interval '1 day' 
           and tsd_timestamp < l_calc_for_date 
         group by t_host_id,t_id,t_name

      union -- get previous day too, for delta calculations !!!

        select 
		t_host_id, 
		t_id,
		t_name,
		max(tsd_timestamp) as ts_date, 
		max(tsd_table_size) as ts_table_size, 
		max(tsd_index_size) as ts_index_size,
		max(tsd_seq_scans) as ts_seq_scans, 
		max(tsd_index_scans) as ts_index_scans
          from table_size_data 
         inner join "tables" 
            on tsd_table_id = t_id
         where tsd_timestamp >= l_calc_for_date - interval '2 day' 
           and tsd_timestamp < l_calc_for_date - interval '1 day' 
         group by t_host_id,t_id,t_name
     ) t;

  create index tbls_idx on tmp_tables_summary (ts_table_id);


  delete from tables_summary 
  where exists 
        ( select 1 
           from tmp_tables_summary
          where tmp_tables_summary.ts_host_id = tables_summary.ts_host_id and
		tmp_tables_summary.ts_table_name = tables_summary.ts_table_name and
		tables_summary.ts_date = l_calc_for_date - interval '1 day' and 
		tables_summary.ts_is_final = 'false');

--return;
  insert into tables_summary (
	ts_host_id, 
	ts_table_name, 
	ts_date,
	ts_table_size,
	ts_index_size,
	ts_seq_scans,
	ts_index_scans,
	ts_orig_table_size,
	ts_orig_index_size,
	ts_is_final
  )
  select 
	curr.ts_host_id, 
	curr.ts_table_name, 
	curr.ts_date::date,
	sum(case when (curr.ts_table_size - coalesce(prev.ts_table_size,0) < 0) then curr.ts_table_size		-- <0 can happen when table is new
	         else curr.ts_table_size - coalesce(prev.ts_table_size,0)
	    end) as ts_table_size,
	sum(case when (curr.ts_index_size - coalesce(prev.ts_index_size,0) < 0) then curr.ts_index_size		-- <0 can happen when table is new
	         else curr.ts_index_size - coalesce(prev.ts_index_size,0)
	    end) as ts_index_size,
	sum(case when (curr.ts_seq_scans - coalesce(prev.ts_seq_scans,0) < 0) then curr.ts_seq_scans		-- <0 can happen when table is new
	         else curr.ts_seq_scans - coalesce(prev.ts_seq_scans,0)
	    end) as ts_seq_scans,
	sum(case when (curr.ts_index_scans - coalesce(prev.ts_index_scans,0) < 0) then curr.ts_index_scans		-- <0 can happen when table is new
	         else curr.ts_index_scans - coalesce(prev.ts_index_scans,0)
	    end) as ts_index_scans,
	max(curr.ts_table_size) ts_orig_table_size,
	max(curr.ts_index_size) ts_orig_index_size,
	l_is_final
   from tmp_tables_summary curr
   left join tmp_tables_summary prev 
     on curr.ts_table_id = prev.ts_table_id and
        curr.ts_host_id = prev.ts_host_id
   where curr.ts_date::date = l_calc_for_date - interval '1 day' and
         prev.ts_date::date = l_calc_for_date - interval '2 day'
   group by 
	curr.ts_host_id, 
	curr.ts_table_name, 
	curr.ts_date::date
   having sum(case when (curr.ts_table_size - coalesce(prev.ts_table_size,0) < 0) then curr.ts_table_size	
	         else curr.ts_table_size - coalesce(prev.ts_table_size,0)
	    end) > 0; -- only >0 sizes

END;
$$
  LANGUAGE 'plpgsql';
