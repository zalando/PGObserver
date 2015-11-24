PGObserver
==========

####PGObserver is a battle-tested monitoring solution for PostgreSQL databases, covering almost all metrics provided by the database engine's internal statistics collector.

PGObserver works out of the box with all Postgres databases (including AWS RDS) starting from version 9.0 and does not require installing any server side database extensions nor privileged users for core functionality, making it perfect for developers!
For some metrics though, data gathering wrapper functions (also known as stored procedures) need to be installed on the server being monitored, to circumvent the superuser requirements.

**Monitored metrics include:**

* Stored procedure data
    - number of calls
    - run time per procedure
    - self time per procedure
* All executed statements data (*)
    - query runtimes, call counts, time spent on IO
* Table IO statistics
    - number of sequential and index scans
    - number of inserts, (hot) updates, deletes
    - table and index size
    - heap hits vs disk hits
* CPU load (**)
* WAL volumes
* General database indicators (number of backends, exceptions, deadlocks and temporary files written)
* Monitoring of schema usage (procedure calls + IUD)
* Monitoring of index usage

\* based on pg_stat_statements module, which needs to be enabled on the DB

\*\* needs a plpythonu stored procedure from sql/data_collection_helpers folder (plpythonu not available on RDS)


**Extra features** (*)

* Monitoring of blocked processes (needs a cron script on the host DB)
* Monitoring of pg_stat_statements (needs pg_stat_statements extension to be enabled)
* Cron Aggregations for speeding up sproc load and database size graphs (useful when monitoring tens of instances)
* Exporting of metrics data to [InfluxDB](https://influxdb.com/) where it can be used for custom charting/dashboarding for example with [Grafana](http://grafana.org/)

\* extra features require some additional setup, look into individual "extra_features" subfolders for instructions

Gathering of single metrics is configurable per instance (see "hosts" table), so if you don't need so much details you don't have to gather them.

Metrics are gathered by a Java application, querying the different PostgreSQL performance views (pg_stat_*).
Gathering intervals for the different metrics can be configured on a per host, per metric basis, enabling more detailed monitoring for critical systems and less details for less important systems, thus reducing the amount of data gathered. Additionally you can configure sets of hosts to monitor from different Java processes, e.g., if you deploy to multiple locations with limited accessibility.

The Web Frontend is a standalone Python + [CherryPy](http://www.cherrypy.org/) application. See the "screenshots" folder for basic examples. Chart visualization is rendered using JS [Flot](http://www.flotcharts.org/) library.

A testdata.py (frontends/src) script is included to generate minimalistic test data for a local test setup.

Visit us at [tech.zalando.com](https://tech.zalando.com/) or feel free to create issues here on Github

Quick Test run using Vagrant
---------------------

Make sure you run the latest [Vagrant](https://www.vagrantup.com/) version on your system.

Clone PGObserver to the machine where you want to run it using Vagrant and run from the PGObserver base directory:


    git clone https://github.com/zalando/PGObserver.git
    cd PGObserver
    vagrant up


This will take a while, doing the following inside the VM:
 * Fetch and start official PostgreSQL 9.3 Docker image
 * Compile the gatherer for you, create a Docker image and start it inside the VM
 * Create a Docker image for the frontend and start it inside the VM
 * Expose port 38080 and 38081 for frontend and gatherer respectively.

You can then open the frontend on port 38080 and configure a database cluster to be monitored, e.g.: http://localhost:38080/hosts/

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
