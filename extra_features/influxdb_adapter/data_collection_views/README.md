## These views are "adapters" to feed the InfluxDB 

All columns except "timestamp" and "host_id" will be pushed into a series named by a "mapping dict"
VIEW_TO_SERIES_MAPPING in influxdb_importer.py

Additionally some more complex data collection queries (where views couldn't use partition exclusion correctly)
are define as templated queries (parameters substituted on runtime) in  the "data_collection_sql_templates" folder.
