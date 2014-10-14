Agregation crons
===

**Feature makes only sense when you're monitoring tens of DB instances!**

Affects only the "frontend" and speeds up page loads.

Currently the frontend "load" graphing relies on some windowing queries that can get slow if DB size is considerable
and longer timeperiods are graphed. The aggregation Crons will materialize windowing queries into real tables.

Setup
-----

* Run the "sql" file on your PgObserver DB - 2 tables for storing  will be created
* Move the python files to a host that is able to run Cron and set it up to run once an hour
* Set the feature flag to "true" in the ~/.pgoserver.conf (or ~/.pgoserver.yaml) file [features -> run_aggregations]
* Restart the "frontend"
