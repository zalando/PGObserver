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
create index on blocking_locks (bl_timestamp desc, bl_host_id, pid);

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
create index on blocking_processes (bp_timestamp desc, bp_host_id, pid);

