RESET role;

SET search_path TO monitor_data, public;

SET role TO pgobserver_gatherer;

-- !! first stat record in a new api schema should write here
-- detect change by new hash value, OR, started usage in a new schema  --- should be inserted by whoever gathers the stats!!
create table sproc_change_detected (
  scd_host_id		integer,
  scd_sproc_name	text,
  scd_schema_name	text,
  scd_is_new_hash	boolean default 'false', -- otherwise, just new API...
  scd_detection_time	timestamp without time zone default now(),
  PRIMARY KEY		( scd_host_id, scd_sproc_name, scd_detection_time )
);


create table host_clusters (
  hc_one_host_id	integer,
  hc_host_id		integer,
  PRIMARY KEY (hc_one_host_id, hc_host_id)
);


create table shard_pairs (
  shard_1st_host_id	integer,
  shard_2nd_host_id	integer,
  PRIMARY KEY (shard_1st_host_id, shard_2nd_host_id)
);


create table monitoring_configuration (
  mc_config_name	text PRIMARY KEY,
  mc_config_value	text,
  mc_config_desc	text
);


create table sprocs_summary (  -- CROSS schemas, per host
  ss_host_id		integer,
  ss_sproc_name		text,
  ss_date		date,
  ss_hour		smallint,
  ss_calls		bigint,
  ss_total_time		bigint,
  ss_orig_calls		bigint,
  ss_orig_total_time	bigint,
  ss_is_final		boolean,
  ss_is_suspect		boolean,
  PRIMARY KEY 		( ss_host_id, ss_sproc_name, ss_date, ss_hour )
);


create table tables_summary (
  ts_host_id		integer,
  ts_table_name		text,
  ts_date		date,
  ts_table_size		bigint, 
  ts_index_size		bigint,
  ts_seq_scans		bigint,
  ts_index_scans	bigint,
  ts_orig_table_size	bigint,
  ts_orig_index_size	bigint,
  ts_is_final		boolean,
  PRIMARY KEY		( ts_host_id, ts_table_name, ts_date )
);


create table hosts_summary (
  hs_host_id		integer,
  hs_host_name		text,
  hs_date		date,
  hs_cache_misses	bigint,
  hs_total_cache_misses	bigint,
  hs_is_final		boolean,
  PRIMARY KEY 		( hs_host_id, hs_date )
);



-- processing
---- if previous hour does not exist, calc it
------- dynamically calc current hour SINCE NEW VERSION (if new version in past hour) per proc, use it (with number of minutes, extarpulate to ONE, compare to one hour in the past)



create table abnormality_test ( 
  at_id			serial PRIMARY KEY,
  at_name		text,
  at_creation_time	timestamp without time zone default now()
);


create table abnormality_findings ( 
  af_detection_time		timestamp without time zone default now(),
  af_abnormality_test_id	integer,
  af_host_id			integer,
  af_date			timestamp,
  af_hour			integer,
  af_is_sproc			boolean default 'true',
  af_object_name		text,
  PRIMARY KEY			( af_detection_time, af_abnormality_test_id, af_object_name )
);


create table performance_ignore_list (
  pil_host_id		integer null,
  pil_object_name	text null
);



