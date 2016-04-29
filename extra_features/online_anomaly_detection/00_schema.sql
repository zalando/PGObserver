create schema olad;

alter default privileges in schema olad grant all on tables to robot_pgo_olad;
grant usage on schema olad to robot_pgo_olad;
grant usage on schema monitor_data to robot_pgo_olad;
grant select on all tables in schema monitor_data to robot_pgo_olad;

set search_path to olad, monitor_data;


create table olad.metrics(
  m_metric text primary key,
  m_description text not null,
  m_enabled boolean not null default true,
  m_last_modified_by text not null default user,
  m_last_modified_on timestamptz not null default now()
);

-- in "continous" mode and with no host_id's specified from command line active hosts from this table will be monitored
create table olad.monitored_hosts(
  mh_host_id int primary key references hosts(host_id) on delete cascade,
  mh_enabled boolean not null default true,
  mh_last_modified_by text not null default user,
  mh_last_modified_on timestamptz not null default now()
);

create table olad.global_default_params(
  gdp_id serial primary key,
  gdp_params json not null,
  gdp_last_modified_by text not null default user,
  gdp_last_modified_on timestamptz not null default now()
);
create unique index default_params_expr_idx on olad.global_default_params((1));

create table olad.metric_default_params(
  mdp_metric text not null primary key,
  mdp_params json not null,
  mdp_last_modified_by text not null default user,
  mdp_last_modified_on timestamptz not null default now()
);

create table olad.host_params(
  hp_host_id int primary key references hosts(host_id) on delete cascade,
  hp_params json not null,
  hp_last_modified_by text not null default user,
  hp_last_modified_on timestamptz not null default now()
);

create table olad.host_metric_params(
  hmp_host_id int primary key references hosts(host_id) on delete cascade,
  hmp_metric text not null,
  hmp_params json not null,
  hmp_last_modified_by text not null default user,
  hmp_last_modified_on timestamptz not null default now()
);

create table olad.unknown_patterns(
  up_host_id int not null references hosts(host_id) on delete cascade,
  up_metric text not null,
  up_ident text not null,
  up_created_on timestamptz not null default now(),
  up_change_pct int not null,
  up_change_pct_allowed int not null,
  up_tz1 timestamptz not null,
  up_tz2 timestamptz,
  up_points int,
  up_metrics json,
  up_message text
);
create index on olad.unknown_patterns (up_created_on);

-- TODO needs a UI for providing a manual feedback possibility to say that some pattern is OK
create table olad.user_acknowleged_patterns(
  uap_host_id int not null references hosts(host_id) on delete cascade,
  uap_metric text,
  uap_ident text,
  uap_created_on timestamptz not null default now(),
  uap_created_by text not null default user,
  uap_metrics json not null,
  uap_tz1 timestamptz,   -- when dates are specified pattern will be ignored only in this window
  uap_tz2 timestamptz,
  uap_tr1 time,         -- specify an additional times within [uap_tz1, uap_tz2] range
  uap_tr2 time
);
create index on olad.unknown_pattern_exceptions (uap_created_on);

-- in metric_ident_filter table will be listed ident's that are currently under monitoring
-- NB! applies only for metrics where '_filter.sql' files are defined
create table olad.metric_ident_filter(
  mif_host_id int not null references hosts(host_id) on delete cascade,
  mif_metric text not null,
  mif_ident text not null,
  mif_created_on timestamptz not null default now(),
  mif_is_permanent boolean default false,    -- can be manually set to always include this ident
  primary key (mif_host_id, mif_metric, mif_ident)
);
create index on olad.metric_ident_filter (mif_created_on);


insert into olad.global_default_params values (default, '{
    "MIN_CLUSTER_ITEMS": 3,
    "MIN_HISTORY_BEFORE_ALERTING_DAYS": 7,
    "LOOKBACK_HISTORY_DAYS": 30,
    "MAX_PATTERN_LENGTH_MINUTES": 3600,
    "CLUSTER_DIFFERENCE_ERROR_PCT": 25,
    "BREAKOUT_THRESHOLD_PCT": 15,
    "ABNORMAL_JUMP_THRESHOLD": 50,
    "MVA_ITEMS": 3,
    "CLUSTER_DIFFERENCE_CALC_WEIGHTS" : {"avg": 20, "med": 10, "stddev": 10, "min": 15, "max": 15, "itemcount": 10, "trend": 20},
    "EMAIL_ADDRESSES": "",
    "DROP_SPIKES": false,
    "NO_ZEROES": false,
    "NO_FILTERING": false,
    }', current_user);

insert into olad.metrics values ('cpu_load', 'cpu load 5m avg'), ('seq_scans', 'seq. scan 1h rate'), ('sproc_runtime', 'sproc runtime (total) per call');