PGObserver
==========

PGObserver is a battle-tested monitoring solution for your PostgreSQL databases. It covers almost all the metrics provided by the database engine's internal statistics collector.

###PGObserver Is Easy-to-Use
PGObserver works out of the box with all PostgreSQL databases (starting from Version 9.0) as well as AWS RDS. You don’t have to install any non-standard, server-side database extensions to take advantage of its core functionality, nor do you need to register any privileged users.

For some metrics, you will have to install data-gathering wrapper functions (also known as stored procedures) on the server being monitored. This will enable you to circumvent the superuser requirements.

Go to [PGObserver's project website](https://zalando.github.io/PGObserver/) for some screenshots and a nice illustration by Zalando Tech resident artist [Kolja Wilcke](https://github.com/kolja).

###Monitored metrics Include:
- **Stored procedure data**: number of calls; run time per procedure; self time per procedure
- **All executed statements data (*)**: query runtimes; call counts; time spent on IO
- **Table IO statistics**: number of sequential and index scans, number of inserts, (hot) updates, deletes, table and index size, heap hits vs disk hits
- **General database indicators**: number of backends; exceptions; deadlocks; and temporary files written
- **Schema usage**: procedure calls, IUD
- CPU load (**)
- WAL (XLOG) volumes
- Index usage


\* based on the pg_stat_statements module, which must be enabled on the DB

\*\* needs a plpythonu stored procedure from the sql/data_collection_helpers folder (plpythonu not available on RDS)

###Additional Features 
These require some extra setup (look in the [extra_features subfolders](https://github.com/zalando/PGObserver/tree/master/extra_features) for instructions):

- Monitoring of
    - blocked processes (needs a cron script on the host DB)
    - pg_stat_statements (needs an enabled pg_stat_statements extension)
    - blocked processes (needs a cron script on the host DB)
    - pg_stat_statements (needs pg_stat_statements extension to be enabled)
- Cron Aggregations for speeding up sproc load and database size graphs (useful when monitoring tens of instances)
- Exporting metrics data to [InfluxDB](https://influxdb.com/), where it can be used for custom charting/dashboarding with [Grafana](http://grafana.org/) (or some other tool)

###How PGObserver Works
A Java application gathers metrics by querying PostgreSQL performance views (pg_stat_*). You can configure gathering intervals for the different metrics on a per-host, per-metric basis. This enables you to gather more details for critical systems, and provide fewer details for less-important systems — thereby reducing the amount of data stored. 

Additionally, you can configure sets of hosts to monitor from different Java processes — for example, when deploying to multiple locations with limited accessibility.

PGObserver’s frontend is a standalone Python + [CherryPy](http://www.cherrypy.org/) application; see the ["screenshots" folder](https://github.com/zalando/PGObserver/tree/master/screenshots) for basic examples. Charts are rendered with the JS [Flot](http://www.flotcharts.org/) library.

To help you generate generate minimalistic test data for a local test setup, we’ve included a testdata.py (frontends/src) script.

###Quick Test Run Using Vagrant

Make sure you've installed the latest [Vagrant](https://www.vagrantup.com/) version on your system. Use Vagrant to clone PGObserver to the machine where you want to run it. Then run from the PGObserver base directory:


    git clone https://github.com/zalando/PGObserver.git
    cd PGObserver
    vagrant up


This last step will take a while, as PGObserver performs the following inside the virtual machine:
- Fetches and starts an official PostgreSQL 9.3 Docker image
- Compiles the gatherer for you, creates a Docker image, and starts it inside the VM
- Creates a Docker image for the frontend and starts it inside the VM
- Exposes ports 38080 and 38081 for the frontend and the gatherer, respectively. You can then open the frontend on port 38080 and configure a database cluster to be monitored — e.g., http://localhost:38080/hosts/

The easiest way to run it somewhere else is to change the config files and create your own Docker images to deploy. Just point it to the PostgreSQL cluster where you created the PGObserver database.

###Setup

Install:
- Python 2.7 (to run PGObserver’s frontend)
- Pip (to prepare Python dependencies)

```
pip install -r frontend/requirements.txt
```
- the PostgreSQL contrib modules [pg_trgm](http://www.postgresql.org/docs/current/static/pgtrgm.html) and [btree_gist](https://github.com/postgres/postgres/tree/master/contrib/btree_gist). These should come with your operating system distribution in a package named postgresql-contrib (or similar).

Create a schema by executing the sql files from your sql/schema folder on a Postgres database where you want to store monitoring data:

```
cat sql/schema/*.sql | psql -1 -f - -d my_pgobserver_db
```

###Configuration

Start by preparing your configuration files for gatherer and frontend. `pgobserver_gatherer.example.yaml` and `pgobserver_frontend.example.yaml` are good starting points.
- set your database connection parameters: name, host and port
- configure the usernames and passwords for gatherer and frontend (the defaults are set in `00_schema.sql`)
- set gather_group (important for gatherer only, enables many gatherer processes)
- create an unprivileged user on the database you want to monitor; to do selects from the system catalogs   

####Configuring Hosts to Monitor
You can either:
- Insert an entry to the monitor_data.hosts table to include the connection details and to-be-monitored features of the cluster you want to monitor (include the same password that you used in the previous step); **OR** 
- use the "frontend" web application's (next step) /hosts page—inserting all needed data and pressing "add", followed by "reload" to refresh menus
    - set host_gather_group to decide which gatherer monitors which cluster
    - To decide which schemas are scanned for sprocs statistics, review the table sproc_schemas_monitoring_configuration. Defaults are provided.

Some features will require you to create according helper functions on the databases being monitored:
- CPU load monitoring requires a stored procedure from "sql/data_collection_helpers/cpu_load.sql". This is a plpythonu function, so a superuser is needed.
- pg_stat_statement monitoring requires "sql/data_collection_helpers/get_stat_statement.sql".
- Table & index bloat query requires "sql/data_collection_helpers/Bloated_tables_and_indexes.sql".
- Blocking processes monitoring requires setup from the "blocking_monitor" folder.

Run the frontend by going into "frontend" folder and running run.sh, which does a "python src/web.py" and puts it in the background:

```
frontend$ ./run.sh --config frontend.yaml
```

Build the data gatherer single jar, including dependencies, by going to the "gatherer" folder and running:

```
mvn clean verify assembly:single
```

Start data monitoring daemons by running run.sh. 
 
###Troubleshooting Hint
----
You might have to change your PostgreSQL server configuration to gather certain types of statistics. Please refer to the Postgres documentation on [The Statistics Collector](http://www.postgresql.org/docs/9.3/static/monitoring-stats.html) and [pg_stat_statements](http://www.postgresql.org/docs/9.3/static/pgstatstatements.html).

###License
Copyright 2012 Zalando GmbH

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
