create unlogged table blocking_monitor_tmp1 (
  pid		integer,
  database	oid,
  relation	oid,
  virtualxid	text,
  transactionid	xid,
  classid	oid,
  objid		oid,
  objsubid	smallint
);

create unlogged table blocking_monitor_tmp2 (
  pid		integer,
  database	oid,
  relation	oid,
  virtualxid	text,
  transactionid	xid,
  classid	oid,
  objid		oid,
  objsubid	smallint
);

create unlogged table blocking_monitor_tmp3 (
  pid		integer
);


