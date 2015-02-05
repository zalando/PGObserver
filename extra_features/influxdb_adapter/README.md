## InfluxDB exporter

This separate "extension" to PgObserver aims to provide "free graphing" based on already existing data.
As Grafana (being one of the most promising graphing dashboards) supports only Graphite, InfluxDB & OpenTSDB, we
chose InfluxDB as it seems to be the most light-weight from these.


### Local setup (for live you probably want a dedicated server)

 - Fire up the vagrant box with "vagrant up" ( requires Vagrant and Virtualbox)
 - Edit the influx_config.yaml to reflect your existing PgObserver setup
 - Start the "export_to_influxdb.py" script in daemon mode
 - Create and use your dashboards at http://0.0.0.0:8082

### Vagrant box connect details

  - Grafana URL - http://0.0.0.0:8082
  - InfluxDB Frontend URL - http://0.0.0.0:8083 (root/root)
  - InfluxDB API URL - http://0.0.0.0:8086


### More info on InfluxDB and Grafana:

[http://influxdb.com/](http://influxdb.com/)

[http://grafana.org/](http://grafana.org/)
