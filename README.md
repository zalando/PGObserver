PGObserver
==========

PGObserver is a battle-tested monitoring solution for your PostgreSQL databases. It covers almost all the metrics provided by the database engine's internal statistics collector, and works out of the box with all PostgreSQL versions (beginning with 9.0) as well as [AWS RDS](https://aws.amazon.com/rds/). You don’t have to install any non-standard, server-side database extensions to take advantage of its core functionality, nor do you need to register any privileged users.

### Monitored Metrics Include:
- **Stored procedure data**: number of calls, run time per procedure, self time per procedure
- **All executed statements data**: query runtimes, call counts, time spent on IO
    - based on the pg_stat_statements module, which must be enabled on the DB
- **Table IO statistics**: number of sequential and index scans, number of inserts, (hot) updates, deletes, table and index size, heap hits vs disk hits
- **General database indicators**: number of backends, exceptions, deadlocks, temporary files written
- **Schema usage**: procedure calls, IUD
- **CPU load**: needs a plpythonu stored procedure from the sql/data_collection_helpers folder (plpythonu isn't available on RDS)
- WAL (XLOG) volumes
- Index usage

For some metrics you must install data-gathering wrapper functions — also known as stored procedures — on the server being monitored. This will enable you to circumvent the superuser requirements.

Go [here](https://zalando.github.io/PGObserver/) for some PGObserver screenshots and a nice illustration by Zalando Tech resident artist [Kolja Wilcke](https://github.com/kolja).

### Additional Features 
With some extra setup ([see instructions](https://github.com/zalando/PGObserver/tree/master/extra_features)), you can also:

- monitor blocked processes (needs a cron script on the host DB)
- monitor pg_stat_statements (needs an enabled pg_stat_statements extension)
- Do cron aggregations for speeding up sproc load and database size graphs; these are useful when monitoring tens of instances
- export metrics data to [InfluxDB](https://influxdb.com/) for custom charting/dashboarding with [Grafana](http://grafana.org/) or some other tool

### Status

Still in use but does not receive active attention or development as stored procedure usage has dropped in new projects.

### How PGObserver Works
A Java application gathers metrics by querying PostgreSQL performance views (pg_stat_*). You can configure gathering intervals for the different metrics on a per-host, per-metric basis. This enables you to gather more details for critical systems and provide fewer details for less-important systems — thereby reducing the amount of data stored. 

Additionally, you can configure sets of hosts to monitor from different Java processes — for example, when deploying to multiple locations with limited accessibility.

PGObserver’s frontend is a standalone Python + [CherryPy](http://www.cherrypy.org/) application; the ["screenshots" folder](https://github.com/zalando/PGObserver/tree/master/screenshots) includes basic examples. Charts are rendered with the JS [Flot](http://www.flotcharts.org/) library.

To help you generate generate minimalistic test data for a local test setup, we’ve included [this] (https://github.com/zalando/PGObserver/blob/master/frontend/src/testdata.py) script.

### Quick Test Run Using Vagrant

Make sure you've installed the latest version of [Vagrant](https://www.vagrantup.com/). Use Vagrant to clone PGObserver to the machine where you want to run it. Then run from the PGObserver base directory:


    git clone https://github.com/zalando/PGObserver.git
    cd PGObserver
    vagrant up


This last step will take a while, as PGObserver performs the following inside the virtual machine:
- Fetches and starts an official PostgreSQL 9.3 Docker image
- Compiles the gatherer for you, creates a Docker image, and starts it inside the VM
- Creates a Docker image for the frontend and starts it inside the VM
- Exposes ports 38080 and 38081 for the frontend and the gatherer, respectively. You can then open the frontend on port 38080 and configure a database cluster to monitor — e.g., http://localhost:38080/hosts/

The easiest way to run it somewhere else is to change the config files and create your own Docker images to deploy. Just point it to the PostgreSQL cluster where you created the PGObserver database.

### Setup

Install:
- Python 2.7 (to run PGObserver’s frontend)
- Pip (to prepare Python dependencies)

```
pip install -r frontend/requirements.txt
```
- the PostgreSQL contrib modules [pg_trgm](http://www.postgresql.org/docs/current/static/pgtrgm.html) and [btree_gist](https://github.com/postgres/postgres/tree/master/contrib/btree_gist). These should come with your operating system distribution in a package named postgresql-contrib (or similar).

Create a schema by executing the SQL files from your sql/schema folder on a Postgres database where you want to store monitoring data:

```
cat sql/schema/*.sql | psql -1 -f - -d my_pgobserver_db
```

### Configuration

Start by preparing your configuration files for [gatherer](https://github.com/zalando/PGObserver/blob/master/gatherer/pgobserver_gatherer.example.yaml) and [frontend](https://github.com/zalando/PGObserver/blob/master/frontend/pgobserver_frontend.example.yaml); the provided examples are good starting points.
- set your database connection parameters: name, host and port
- configure the usernames and passwords for gatherer and frontend; find defaults [here](https://github.com/zalando/PGObserver/blob/master/sql/schema/00_schema.sql)
- set gather_group (important for gatherer only; enables many gatherer processes)
- create an unprivileged user on the database you want to monitor; to do selects from the system catalogs   

#### Configuring Hosts to Monitor

You can either:
- Insert an entry to the monitor_data.hosts table to include the connection details and to-be-monitored features of the cluster you want to monitor (include the same password that you used in the previous step); **OR** 
- use the "frontend" web application's (next step) `/hosts` page, inserting all needed data and pressing "add", followed by "reload" to refresh menus
    - set `host_gather_group` to decide which gatherer monitors which cluster
    - to decide which schemas are scanned for sprocs statistics, review the table `sproc_schemas_monitoring_configuration`. Defaults are provided.

Some features will require you to create according helper functions on the databases being monitored:
- CPU load monitoring requires a stored procedure from [cpu_load.sql](https://github.com/zalando/PGObserver/blob/master/sql/data_collection_helpers/cpu_load.sql). This is a plpythonu function, so a superuser is needed.
- For pg_stat_statement monitoring, you need [this file](https://github.com/zalando/PGObserver/blob/master/sql/data_collection_helpers/get_stat_statements.sql).
- For table & index bloat query, you need [this](https://github.com/zalando/PGObserver/blob/master/sql/data_collection_helpers/bloated_tables_and_indexes.sql).
- Blocking processes monitoring requires setup from [this folder](https://github.com/zalando/PGObserver/tree/master/extra_features/blocking_monitor).

Run the frontend by going into [the "frontend" folder](https://github.com/zalando/PGObserver/tree/master/frontend) and running [run.sh](https://github.com/zalando/PGObserver/blob/master/frontend/run.sh), which creates a "python src/web.py" and puts it in the background:

```
frontend$ ./run.sh --config frontend.yaml
```

Build the data gatherer single jar, including dependencies, by going to [the "gatherer" folder](https://github.com/zalando/PGObserver/tree/master/gatherer) and running:

```
mvn clean verify assembly:single
```

Start data monitoring daemons by running [run.sh](https://github.com/zalando/PGObserver/blob/master/gatherer/run.sh). 
 
### Troubleshooting Hint
----
You might have to change your PostgreSQL server configuration to gather certain types of statistics. Please refer to the Postgres documentation on [The Statistics Collector](http://www.postgresql.org/docs/9.3/static/monitoring-stats.html) and [pg_stat_statements](http://www.postgresql.org/docs/9.3/static/pgstatstatements.html).

### Contributions
PGObserver welcomes contributions to the community. Please go to the [Issues](https://github.com/zalando/pgobserver/issues) page to learn more about planned project enhancements and noted bugs. Feel free to make a pull request and we'll take a look.

### Thank You
Thank you to our Zalando contributors, as well as Fabian Genter.

### License
Copyright 2012 Zalando GmbH

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
