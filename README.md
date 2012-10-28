PGObserver
==========

Tool to monitor PostgreSQL performance metrics including:

 * Stored procedure data
 * Table IO statistics
 * CPU load

Data is gathered by a single Java application. Gather ntervals for the different metrics can be set on a per host per metric basis.

The frontend is a Python 2.7 CherryPy app.

A testdata.py script is included to generate minimalistic testdata.

Setup
-----

 * Create schema from sql/schema.sql
 * Copy frontend/default_config.conf to home folder
 * Edit monitor_data.hosts table to include the databases you want to monitor
 * To connect to a database create only an unprivileged user, this user will be able to read all the required data
 * For CPU load created the stored procedure from sql/cpuload.sql , this is a plpythonu function

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
