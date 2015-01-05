PGObserver
==========

PGObserver was developed at Zalando during the past years to monitor performance metrics of our different PostgreSQL clusters. Due to our use of a stored procedure API layer our strong focus was on monitoring that API layer. However the number of metrics was increased to include other relevant values:

* Stored procedure data (pure sql procedures are not tracked)
    - number of calls
    - run time per procedure
    - self time per procedure
* Table IO statistics
    - number of sequential and index scans
    - number of inserts, (hot) updates, deletes
    - table and index size
    - heap hits vs disk hits
* CPU load
* WAL volumes
* General database indicators (number of backends, exceptions, deadlocks and temporary files written)
* Monitoring of schema usage (procedure calls + IUD)
* Monitoring of index usage

Extra features ( require some additional setup, look into "extra_features" folder)

* Monitoring of blocked processes (needs a cron script on the host DB)
* Monitoring of pg_stat_statements (needs pg_stat_statements extension to be enabled)
* Cron Aggregations for speeding up sproc load and database size (useful when monitoring tens of instances)

Note: gathering of single metrics is configurable per instance ("hosts" table), so if you don't need so much details you don't have to gather them

The performance data is gathered by a Java application querying the different PostgreSQL performance views. Gather intervals for the different metrics can be configured on a per host per metric basis, enabling more detailed monitoring for critical systems and less details for less important systems, thus reducing the amount of data gathered. Additionally you can configure sets of hosts to monitor from different Java processes, e.g., if you deploy to multiple locations with limited accessability.

The web frontend is a Python CherryPy standalone application. See the screenshot folder for basic examples. Chart visualization is rendered using JS flot library.

A testdata.py (frontends/src) script is included to generate minimalistic test data for a local test setup.

Visit us at http://tech.zalando.com or feel free to create issues on Github

Running using Vagrant
---------------------

Make sure you run the latest Vagrant version on your system (vagrantup.com)

Clone PGObserver to the machine where you want to run it using Vagrant and run from the PGObserver base directory:

```
vagrant up
```

This will take a moment, doing the following inside the VM:
 * Install PostgreSQL 9.3
 * Compile the gatherer for you, create a Docker image and start it inside the VM
 * Create a Docker image for the frontend and start it inside the VM
 * Expose port 38080 and 38081 for frontend and gatherer respectively.

You can then open the frontend on port 38080 and configure a database cluster to be monitored. Data is stored within the VM.

If you want to run it somewhere else, the easy way would be to change the config files and create your own Docker images to deploy. Basically just point it to a configure PostgreSQL cluster, where you created the PGObserver database.

Setup
-----
 * PGObserver frontend is developed/run using Python 2.7
 * Prepare Python dependencies using pip

```
pip install -r frontend/requirements.txt
```

 * Create schema by executing the sql files from sql/schema folder on a Postgres DB where you want to store your monitoring data

```
psql -f sql/schema/00_schema.sql [ 01_...]
```

 * Copy pgobserver.yaml to home folder ~/.pgobserver.yaml (if you're running the gatherer and the frontend on different machines you need it on both)

```
cp pgobserver.yaml ~/.pgobserver.yaml
```

 * Configure .pgobserver.yaml to match your setup
 	- set database where to store data
 	- configure usernames and passwords
    - set gather_group (important for gatherer only, enables many gatherer processes)
 * Create an unprivileged user on the database you want to monitor for doing selects from the system catalogs
 * Configure hosts to be monitored
    - insert an entry to monitor_data.hosts table to include the connection details and to-be-monitored features for the cluster you want to monitor (incl. password from previous step)
    OR do it via the "frontend" web application's (next step) /hosts page, by inserting all needed data and pressing "add" (followed by "reload" to refresh menus)
    - set host_gather_group to decide which gatherer monitors which cluster
    - for deciding which schemas are scanned for sprocs statistics review the table sproc_schemas_monitoring_configuration (defaults are provided)
 * For some features you need to create according helper functions on the databases being monitored
    - CPU load monitoring requires stored procedure from "sql/data_collection_helpers/cpu_load.sql" (this is a plpythonu function, so superuser is needed)
    - pg_stat_statement monitoring requires "sql/data_collection_helpers/get_stat_statement.sql"
    - Table & index bloat query requires "sql/data_collection_helpers/Bloated_tables_and_indexes.sql"
    - Blocking processes monitoring requires setup from the "blocking_monitor" folder
 * Run the frontend by going into "frontend" folder and running run.sh (which does a "python src/web.py")
 * Build the data gatherer single jar including dependencies by going to "gatherer" folder and running

```
mvn clean verify assembly:single
```

 * Start data monitoring daemon(s) by running run.sh (which does a "java -jar target/PGObserver-Gatherer-1.${CUR_VER}-SNAPSHOT-jar-with-dependencies.jar")

Hint
----

To enable the gathering of certain statistics the PostgreSQL server configuration may need changes, refer to: http://www.postgresql.org/docs/9.3/static/monitoring-stats.html and http://www.postgresql.org/docs/9.3/static/pgstatstatements.html


License
-------

Copyright 2012 Zalando GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
