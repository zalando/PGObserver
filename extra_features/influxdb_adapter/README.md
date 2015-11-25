## InfluxDB exporter

This separate "extension" to PgObserver aims to provide "free graphing" based on already existing data.
As Grafana (being one of the most promising graphing dashboards) supports only Graphite, InfluxDB & OpenTSDB, we
chose InfluxDB as it seems to be the most light-weight from these.


### Local setup (for live you probably want a dedicated server)

 - Fire up the vagrant box (from "vagrant_influx_grafana_box" folder) with "vagrant up" ( requires Vagrant and Virtualbox)
 - Edit the influx_config.yaml to reflect your existing PgObserver setup (DB connection strings info + retention period + optionally selection of metrics you want to sync)
 - Start the "export_to_influxdb.py" script in daemon mode (this builds up the history up to configured days and will keep polling the Postgres metrics DB for fresh data, pushing it to Influx when found)
 - Create and use your dashboards at http://0.0.0.0:3000 (follow the final output lines of "vagrant up" to set up a datasource)

### Vagrant box connect details

  - Grafana URL - http://0.0.0.0:3000 (admin/admin)
  - InfluxDB Frontend URL - http://0.0.0.0:8083 (root/root)
  - InfluxDB API URL - http://0.0.0.0:8086 (used internally by Grafana)


### More info on InfluxDB and Grafana:

[http://influxdb.com/](http://influxdb.com/)

[http://grafana.org/](http://grafana.org/)
