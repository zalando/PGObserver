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

PGObserver is release under the Apache License Version 2.0 see LICENSE-2.0.txt
