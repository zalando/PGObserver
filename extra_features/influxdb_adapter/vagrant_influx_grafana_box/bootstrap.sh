#!/bin/sh -e

echo "starting InfluxDB + Grafana setup..."

export DEBIAN_FRONTEND=noninteractive

apt-get update
echo "Installing Packages ..."
echo "apt-get install -y jq vim libfontconfig1-dev sqlite3"
apt-get install -y jq vim libfontconfig1-dev sqlite3

echo ""
echo ""

echo "Downloading Influxdb ..."
echo "Determining the latest version from Github..."
echo "curl -Gsq 'https://api.github.com/repos/influxdata/influxdb/tags'"
curl -Gsq 'https://api.github.com/repos/influxdata/influxdb/tags' > influxdb_tags.json
INFLUX_VER=$( jq .[0].name influxdb_tags.json | grep -o '[0-9].*[0-9]' )
echo "Found ver. ${INFLUX_VER} ..."
INFLUX_PKG=influxdb_${INFLUX_VER}_amd64.deb
INFLUX_URL=https://s3.amazonaws.com/influxdb/$INFLUX_PKG
wget -nc -q $INFLUX_URL
dpkg -i $INFLUX_PKG
echo "Starting Influxdb ..."
service influxdb start
sleep 10
echo "OK!"

echo ""
echo ""

echo "Creating 'pgobserver' database on Influxdb..."
curl -Gq http://localhost:8086/query --data-urlencode "q=create database pgobserver"
curl -Gq http://localhost:8086/query --data-urlencode "q=show databases"
echo ""
echo "Creating our 30d retention policy - needs explicit specification when writing data points!"
curl -Gq http://localhost:8086/query --data-urlencode "q=create retention policy \"30d\" on pgobserver duration 30d replication 1 default"
curl -Gq http://localhost:8086/query --data-urlencode "q=show retention policies on pgobserver"
echo ""
echo ""
echo ""

echo "Downloading Grafana..."
echo "Determining the latest version from Github..."
echo "curl -Gsq 'https://api.github.com/repos/grafana/grafana/tags'"
curl -Gsq 'https://api.github.com/repos/grafana/grafana/tags' > grafana_tags.json
GRAFANA_VER=$( jq .[0].name grafana_tags.json | grep -o '[0-9].*[0-9]' )
echo "Found ver. ${GRAFANA_VER} ..."
GRAFANA_PKG=grafana_${GRAFANA_VER}_amd64.deb
GRAFANA_URL=https://grafanarel.s3.amazonaws.com/builds/$GRAFANA_PKG
echo "Downloading Grafana from $GRAFANA_URL ..."
wget -nc -q $GRAFANA_URL
dpkg -i $GRAFANA_PKG
# apt-get -f install -y  # fixes some strange dependecy issues with libfontconfig1
update-rc.d grafana-server defaults 95 10   # auto start
service grafana-server start

sleep 5
echo "Creating the documentation dashboard in Grafana..."
cp /vagrant/create_documentation_dashboard.sql .
sqlite3 /var/lib/grafana/grafana.db ".read create_documentation_dashboard.sql"
echo ""
echo "Finished!"
echo ""
echo ""
echo "Grafana URL - http://0.0.0.0:3000 admin/admin"
echo "InfluxDB Frontend URL - http://0.0.0.0:8083"
echo "InfluxDB API URL - http://0.0.0.0:8086 root/root, db=pgobserver"
echo ""
echo "To get going:"
echo ""
echo "   1.) Create in Grafana a 'Data Source' for InfluxDB 0.9.x with aforementioned InfluxDB API connect data"
echo "   2.) Review and change the influx_config.yaml according to your environment"
echo "   3.) Start the python export_to_influxdb.py daemon, pointing your influx_config.yaml to it"
echo "   4.) Have fun graphing!"
