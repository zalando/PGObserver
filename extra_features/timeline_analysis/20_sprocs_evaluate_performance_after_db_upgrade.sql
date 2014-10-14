--select * from hosts where host_name like '%zalos%'
-- select * from sprocs_evaluate_performance_after_db_upgrade()
-- select * from sprocs_evaluate_performance_after_db_upgrade(null,null,null,null,null,null,null,5)
--select * from sprocs_evaluate_performance_after_db_upgrade('{1}')
--select * from sprocs_evaluate_performance_after_db_upgrade('{3,4,11,12,41,42,43,44}')
--select * from sprocs_evaluate_performance_after_db_upgrade('{1}','2014-04-08',NULL,'2014-04-09',NULL,8,10)
--select * from sprocs_evaluate_performance_after_db_upgrade('{6}','2014-03-26',NULL,'2014-04-01',NULL,2,6)
CREATE OR REPLACE FUNCTION sprocs_evaluate_performance_after_db_upgrade(
  IN  p_host_ids_array	        integer array default NULL,
  IN  p_prev_start_date		date default NULL, -- p_data_start_date
  IN  p_prev_end_date		date default NULL, -- p_prev_start_date 
  IN  p_curr_start_date		date default NULL, -- p_data_end_date 
  IN  p_curr_end_date		date default NULL, -- p_curr_start_date 
  IN  p_start_hour		integer default NULL, -- 0 for full day
  IN  p_end_hour		integer default NULL, -- 23 for full day  NOTE, FOR CURR DAY compare, must limit by @12:30 -- 11:00 -- now -1 hour
  IN  p_report_factor     	integer default 2, -- report those that got better/worse more than 200%

  OUT is_better_worse_total    text, -- ' better', ' worse', 'total'
  OUT factor			decimal(9,2),
  OUT sproc_name		text, 
  OUT prev_calls		bigint,
  OUT curr_calls		bigint,
  OUT prev_avg_time		decimal(9,2),
  OUT curr_avg_time		decimal(9,2),
  OUT curr_total_time		bigint
)
  RETURNS setof record AS
--returns void as
$$
DECLARE
  l_data_start_date		date default NULL; -- one week ago - 1day
  l_data_end_date		date default NULL; -- yesterday

BEGIN

  -- by defualt, compare one day - yesterday, and same day a week earlier
--  p_data_start_date := coalesce(p_data_start_date, now()::date - interval '8 days');
--  p_data_end_date   := coalesce(p_data_end_date, now()::date - interval '1 days');
  p_prev_start_date := coalesce(p_prev_start_date, now()::date - interval '8 days');
  p_prev_end_date   := coalesce(p_prev_end_date, p_prev_start_date);
  p_curr_start_date := coalesce(p_curr_start_date, now()::date - interval '1 days');
  p_curr_end_date   := coalesce(p_curr_end_date, p_curr_start_date);
  p_start_hour      := coalesce(p_start_hour,0);
  p_end_hour        := coalesce(p_end_hour,23);
  p_report_factor   := coalesce(p_report_factor,2);


--raise notice '%,%,%,%,%,%',  p_prev_start_date, p_prev_end_date, p_curr_start_date,  p_curr_end_date,  p_start_hour,  p_end_hour;


  drop table if exists sums;
  create temporary table sums (date_ date, sproc_name text, sum_time bigint, sum_calls bigint);

  insert into sums 
  select ss_date, ss_sproc_name, sum(ss_total_time),sum(ss_calls)
    from sprocs_summary 
   where ( p_host_ids_array IS NULL OR ss_host_id  = ANY (p_host_ids_array) )
     and ss_date between p_prev_start_date and p_curr_end_date
     and ss_hour between p_start_hour and p_end_hour
     and not ss_is_suspect 
   group by ss_date,ss_sproc_name;


  drop table if exists sums2;
  create temporary table sums2 (week integer, sproc_name text, sum_time bigint, sum_calls bigint);

  insert into sums2 
  select 1, sums.sproc_name, sum(sum_time), sum(sum_calls)
    from sums
   where date_ between p_prev_start_date and p_prev_end_date
   group by sums.sproc_name;

  insert into sums2 
  select 3, sums.sproc_name, sum(sum_time), sum(sum_calls)
    from sums
   where date_ between p_curr_start_date and p_curr_end_date
   group by sums.sproc_name;



  RETURN QUERY

    select 'worse', 
	   ( (1.0*c.sum_time/c.sum_calls) / (1.0*a.sum_time/a.sum_calls) )::decimal(9,2) factor,
	   a.sproc_name, 
	   a.sum_calls calls_week1, 
	   c.sum_calls calls_week3,
	   (1.0*a.sum_time/a.sum_calls)::decimal(9,2) avg_time_week1,
	   (1.0*c.sum_time/c.sum_calls)::decimal(9,2) avg_time_week3,
	   c.sum_time
     from sums2 a 
    inner join sums2 c 
       on c.sproc_name = a.sproc_name
    where a.week = 1 
      and c.week=3
      and a.sum_time>0 
      and c.sum_time>0 
      and a.sum_calls>0 
      and c.sum_calls>0
      and 1.0*a.sum_time/a.sum_calls * p_report_factor < 1.0*c.sum_time/c.sum_calls

  UNION ALL

    select 'better', 
	   ( (1.0*a.sum_time/a.sum_calls) / (1.0*c.sum_time/c.sum_calls) )::decimal(9,2) factor,
	   a.sproc_name, 
	   a.sum_calls calls_week1, 
	   c.sum_calls calls_week3,
	   (1.0*a.sum_time/a.sum_calls)::decimal(9,2) avg_time_week1,
	   (1.0*c.sum_time/c.sum_calls)::decimal(9,2) avg_time_week3,
	   c.sum_time
     from sums2 a 
    inner join sums2 c 
       on c.sproc_name = a.sproc_name
    where a.week = 1 
      and c.week=3
      and a.sum_time>0 
      and c.sum_time>0 
      and a.sum_calls>0 
      and c.sum_calls>0
      and 1.0*a.sum_time/a.sum_calls > 1.0*c.sum_time/c.sum_calls * p_report_factor

   UNION ALL

    select 'a total', 
           case when 
	     ( (1.0*sum(c.sum_time)/sum(c.sum_calls)) / (1.0*sum(a.sum_time)/sum(a.sum_calls)) )::decimal(9,2) > 1 then 
	       0- ( (1.0*sum(c.sum_time)/sum(c.sum_calls)) / (1.0*sum(a.sum_time)/sum(a.sum_calls)) )::decimal(9,2) else 
	       (1/ ( (1.0*sum(c.sum_time)/sum(c.sum_calls)) / (1.0*sum(a.sum_time)/sum(a.sum_calls)) ))::decimal(9,2)
	    end factor,
	   ' ', 
	   sum(a.sum_calls)::bigint calls_week1, 
	   sum(c.sum_calls)::bigint calls_week3,
	   (1.0*sum(a.sum_time)/sum(a.sum_calls))::decimal(9,2) avg_time_week1,
	   (1.0*sum(c.sum_time)/sum(c.sum_calls))::decimal(9,2) avg_time_week3,
	   sum(c.sum_time)::bigint
     from sums2 a 
    inner join sums2 c 
       on c.sproc_name = a.sproc_name
    where a.week = 1 
      and c.week=3
      and a.sum_time>0 
      and c.sum_time>0 
      and a.sum_calls>0 
      and c.sum_calls>0


  order by 1 asc, 2 desc;

	   
           
END;
$$
  LANGUAGE 'plpgsql';
