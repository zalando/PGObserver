-- this sproc is not part of the automatic processing - it is used manually to get the past results of a sproc suspected to have
-- a problem (one can also find the function's graph in pgObserver)

CREATE OR REPLACE FUNCTION sproc_get_prev_values(
  IN  p_host_id			integer,
  IN  p_sproc_name		text,
  OUT avg_time			integer,
  OUT date_			date,
  OUT hour_			smallint,
  OUT total_time		bigint,
  OUT calls			bigint,
  OUT is_suspect		text
)
  RETURNS setof record AS
$$
BEGIN

  RETURN QUERY
  select (ss_total_time/ss_calls)::integer, 
	 ss_date, 
	 ss_hour, 
	 ss_total_time, 
	 ss_calls, 
	 case when ss_is_suspect then 'suspect'::text else ''::text end
    from sprocs_summary 
   where ss_host_id = p_host_id 
     and ss_sproc_name = p_sproc_name
     and ss_calls > 0
   order by ss_date desc,ss_hour desc
   limit 2000;

END;
$$
  LANGUAGE 'plpgsql';
