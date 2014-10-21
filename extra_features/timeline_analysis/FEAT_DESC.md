Timeline analyzis
===

Feature aims to spot substantial sproc runtimes slowdowns and sending Cron notification emails on findings.

This feature is using the data collected and stored by the normal pgObserver collection mechanism.
Each hour, it calculates the total and per call avg time a function consumed in that hour (pgobserver collects the totals reported by postgresql, so a delta is calculated).
The findings are then compared to the avg of past weeks (same day of the week, same hour) if past weeks (parameter number of weeks). Extreme findings are reported (increase beyond a configurable %).

All configuration settins are in the table monitoring_configuration.



Additional features:
1. the size increases and seq scans of tables are compared on a daily bases (with past week's values). Just seq scans increases were found usefull.
2. the avg run-time of sprocs between 'shards' is compared on a daily bases. Found useful to catch missing indexes, etc. (e.g. due to a rebuild that resulted in an invalid index).
3. the memory cach hit-ratio is compared on a daily bases for each cluster - to find out when increasing database memory is adviced.
4. a funciton that compares function's avg & total run-time for a given time period (set of hours, set of days) to same period in previous weeks. Used mostly to find performance issues after special 'manipulations', like db version upgrade.
 

Search patterns:
  TODO

NB! Not really a real-time solution (1h chunks are analyzed)

 
Setup
-----

* Run the "sql" files on the PgObserver database
* Set up the Crons



