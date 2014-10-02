set search_path to z_blocking;


-- select * from locks_monitor();
CREATE OR REPLACE FUNCTION blocking_monitor(
)
  RETURNS VOID AS
$$

DECLARE
  l_switch integer;
  l_curr_timestamp timestamp without time zone;
  l_locks_loop_runs integer;
  l_runs integer;
  l_cnt1 integer;
  l_cnt2 integer;
  l_cnt3 integer;

BEGIN

  l_switch := 1;
  l_runs := 0;

	select timeofday()
	  into l_curr_timestamp;

	select count(1) 
	  from z_blocking.blocking_monitor_tmp1
	  into l_cnt1;

	if (l_cnt1 = 0) then

	  insert into z_blocking.blocking_monitor_tmp1
	  select 
		pid,	
		database,
		relation,
		virtualxid,
		transactionid,
		classid,	
		objid,	
		objsubid
	   from 
		pg_catalog.pg_locks
	  where NOT granted;

	else   

	  insert into z_blocking.blocking_monitor_tmp2
	  select 
		pid,	
		database,
		relation,
		virtualxid,
		transactionid,
		classid,	
		objid,	
		objsubid
	   from 
		pg_catalog.pg_locks
	  where NOT granted;
	end if;

	-- find presistant lock
	insert into z_blocking.blocking_monitor_tmp3
	select distinct a.pid
	  from z_blocking.blocking_monitor_tmp1 a
	 inner join z_blocking.blocking_monitor_tmp2 b
	    on a.pid = b.pid
	   and (  (a.transactionid is not null and a.transactionid = b.transactionid)
	       or (a.virtualxid is not null and a.virtualxid = b.virtualxid)
	       or (a.classid is not null and a.classid  = b.classid and a.objid = b.objid and a.objsubid = b.objsubid)
	       or (a.database is not null and a.database = b.database and a.relation = b.relation)
	       );

	-- find blocking pids
	insert into z_blocking.blocking_monitor_tmp3
	select a.pid
	  from pg_catalog.pg_locks a
	 inner join z_blocking.blocking_monitor_tmp1 b
	    on a.pid != b.pid
	 inner join z_blocking.blocking_monitor_tmp3 c -- only blocked guys
	    on b.pid = c.pid 
	   and (  (a.transactionid is not null and a.transactionid = b.transactionid)
	       or (a.virtualxid is not null and a.virtualxid = b.virtualxid)
	       or (a.classid is not null and a.classid  = b.classid and a.objid = b.objid and a.objsubid = b.objsubid)
	       or (a.database is not null and a.database = b.database and a.relation = b.relation)
	       );

	select count(1) 
	  from z_blocking.blocking_monitor_tmp3
	  into l_cnt3;

	-- keep record of the blocking !
	if (l_cnt3 > 0) then
	  insert into z_blocking.blocking_locks
	  select 0, l_curr_timestamp, *
	    from pg_catalog.pg_locks
	   where exists (select 1 from z_blocking.blocking_monitor_tmp3 where pg_locks.pid = blocking_monitor_tmp3.pid);

	  insert into z_blocking.blocking_processes
	  select 0, l_curr_timestamp, *
	    from pg_catalog.pg_stat_activity
	   where exists (select 1 from z_blocking.blocking_monitor_tmp3 where pg_stat_activity.pid = blocking_monitor_tmp3.pid);

	  truncate table z_blocking.blocking_monitor_tmp3;
	end if;

	-- cleanup for next time
	if (l_cnt1 = 0) then -- we inseted into blocking_monitor_tmp1, so truncate older table
	  select count(1) 
	    from z_blocking.blocking_monitor_tmp2
	    into l_cnt2;
	  if (l_cnt2 = 0) then 
	    truncate table z_blocking.blocking_monitor_tmp2;
	  end if;
	else   -- we inseted into blocking_monitor_tmp2, so truncate older table (tmp1)
	  truncate table z_blocking.blocking_monitor_tmp1;
	end if;
	  
	l_runs := l_runs +1;

END;
$$
  LANGUAGE 'plpgsql';
