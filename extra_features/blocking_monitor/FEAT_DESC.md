Blocking monitor
===

**Feature is quite resource hungry and is more suitable for analyzing specific locking situations!**

Feature will display an additional line on the WAL graph about "blocking locks" occurances and additional screen (/perflocks)
with details about blocking and blocked queries. Feature affects both the "frontend" and the "gatherer".


Setup
-----

* Run the "sql" files from the "host_setup" on the DB cluster that you want to monitor for blocking locks
* Set up Cron on the monitored DB to run the python from "host_setup" to be executed every minute (best prefixed with flock for additional safety) (* * * * * /usr/bin/flock -n -x /tmp/blocking_monitor.lock blocking_monitor.py --db db_under_monitoring )
* Run the "sql" files from the "pgo_setup" on your PgObserver database
* Set the feature as enabled on the DB hosts you plan to monitor by ensuring there's a "blockingStatsGatherInterval" key with 0 value > 0 in the hosts.host_settings column.
This can be also done via the UI (/hosts). The key determines the frequency of pulling locking data from the monitored host to PgObserver DB.
* Restart the "frontend" (reloading will work also /hosts->Reload Config)
* Restart the "gatherer"
