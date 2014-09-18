PGObserver
==========

PGObserver was developed at Zalando during the past years to monitor performance metrics of our different PostgreSQL clusters. Due to our use of a stored procedure API layer our strong focus was on monitoring that API layer. However the number of metrics was increased to include other relevant values:

 * Stored procedure data
   - number of calls
   - run time per procedure
   - self time per procedure
 * Table IO statistics
   - number of sequential and index scans
   - number of inserts, (hot) updates, deletes
   - table and index size
   - heap hits vs disk hits
 * CPU load

The performance data is gathered by a Java application querying the different PostgreSQL performance views. Gather intervals for the different metrics can be configured on a per host per metric basis, enabling more detailed monitoring for critical systems and less details for less important systems, thus reducing the amount of data gathered. Additionally you can configure sets of hosts to monitor from different Java processes, e.g., if you deploy to multiple locations with limited accessability.

The web frontend is a Python CherryPy standalone application. See the screenshot folder for basic examples. Chart visualization is rendered using JS flot library.

A testdata.py script is included to generate minimalistic test data for a local test setup.

Visit us at http://tech.zalando.com

Setup
-----

 * PGObserver frontend is developed/run using Python 2.7
 * Prepare Python dependencies using pip
```
pip install -r frontend/requirements.txt
```

 * Create schema from sql/schema.sql where you want to store your data
```
psql -f sql/schema.sql [...]
```

 * Put the PGObserver into the users home folder, that will run PGObserver
```
cp pgobserver.conf ~/.pgobserver.conf
```

 * Configure .pgobserver.conf to match your setup
 	- set database where to store data
 	- configure usernames and passwords
 	- set hostgroup ( for gatherer )
    - possible remove/change the additional filter "default_schema_filter" for schemas where you want your sprocs to be found. our default is scan only schemas with _api or _data
 * Create an unprivileged PostgreSQL user on the database you want to monitor
 * Add entry to monitor_data.hosts table to include the databases you want to monitor
 	- use host_group to decide which gatherer monitors which cluster
 * Build a single jar including dependencies using
```
mvn clean verify assembly:single
```
 * Start monitoring with:
```
java -jar target/PGObserver-Gatherer-1.0-SNAPSHOT-jar-with-dependencies.jar
```
 * For CPU load created the stored procedure from sql/cpuload.sql , this is a plpythonu function

Hint
----

To enable the gathering of certain statistics the PostgreSQL server configuration may need changes, refer to: http://www.postgresql.org/docs/9.3/static/monitoring-stats.html

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
