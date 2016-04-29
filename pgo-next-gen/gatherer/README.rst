==============
pgobserver_gatherer
==============

A postgres metrics collection daemon, successor of the original [PGObserver](https://github.com/zalando/PGObserver) Java gathering daemon

NB! BETA


Installation
-----------------

- Bootstrap the database (datastore) that will keep the configuration and metrics data for Postgres databases you want to monitor
- pip3 install --upgrade pgobserver_gatherer
- python3 -m pgobserver_gatherer --init  # to create a config file with connect details to your datastore
- python3 -m pgobserver_gatherer -v  # to run the daemon in verbose mode

Improvements compared to old gatherer
-----------------

- automatic detection of gatherer configuration changes (hosts.host_settings column)
- connection pooling to datastore
- generalized data storing, less boilerplate for gatherers
- single transaction bulk inserts (copy protocol)
- connectivity check to all DBs under monitoring on startup
- single host/single gatherer test mode
- "delta engine" diffing engine for getting real changes
- custom output plugins. separate processes that get incoming datapoints via queues


Operating principles
-----------------

1. gather data
2. store to postgres as currently - optional
3. feed the analytics framework (delta engine) that calculates the diffs for cumulative columns
4. feed the data coming from delta engine to plugins (simplest one being the "console" outputting plugin)
 - config files describing handlers and their formats and wanted datasets. they can have extra config files for output
 - loading of modules and feeding them when data comes in
