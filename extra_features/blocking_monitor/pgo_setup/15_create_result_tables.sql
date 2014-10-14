RESET role;

SET search_path TO monitor_data, public;

SET role TO pgobserver_gatherer;

--drop table blocking_locks;
create table blocking_locks (
  bl_host_id	integer not null,
  bl_timestamp timestamp without time zone not null,
  locktype	text,
  database	oid,
  relation	oid,
  page		integer,
  tuple		smallint,
  virtualxid	text,
  transactionid	text, --xid originally
  classid	oid,
  objid		oid,
  objsubid	smallint,
  virtualtransaction	text,
  pid		integer,
  mode		text,
  granted	boolean,
  fastpath	boolean
);
create index on blocking_locks (bl_timestamp desc);

-- drop table blocking_processes
create table blocking_processes (
  bp_host_id	integer not null,
  bp_timestamp	timestamp without time zone not null,
  datid		oid,
  datname	name,
  pid		integer,
  usesysid	oid,
  usename	name,
  application_name	text,
  client_addr	text, --inet originally
  client_hostname	text,
  client_port	integer,
  backend_start	timestamp with time zone,
  xact_start	timestamp with time zone,
  query_start	timestamp with time zone,
  state_change	timestamp with time zone,
  waiting	boolean,
  state		text,
  query		text
);
create index on blocking_processes (bp_timestamp desc);



DO $$
DECLARE
  l_tables text[] := array['blocking_locks', 'blocking_processes'];
  l_table_prefixes text[] := array['bl', 'bp'];
BEGIN
  FOR i IN 1.. array_upper(l_tables, 1)
  LOOP
    RAISE WARNING 'dropping triggers for: %', l_tables[i];
    EXECUTE 'DROP TRIGGER IF EXISTS ' || l_tables[i] || '_insert_trigger ON ' || l_tables[i];
    RAISE WARNING 'creating triggers for: %', l_tables[i];
    PERFORM monitor_data.create_partitioning_trigger_for_table(l_tables[i], l_table_prefixes[i]);
  END LOOP;
END;
$$
LANGUAGE plpgsql;