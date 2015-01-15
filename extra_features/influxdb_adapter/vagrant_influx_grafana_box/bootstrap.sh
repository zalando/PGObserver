#!/bin/sh -e

echo "starting InfluxDB + Grafana setup..."

export DEBIAN_FRONTEND=noninteractive

apt-get update

echo "Downloading Influxdb ..."
wget -q http://s3.amazonaws.com/influxdb/influxdb_latest_amd64.deb
dpkg -i influxdb_latest_amd64.deb
echo "Starting Influxdb ..."
/etc/init.d/influxdb start
sleep 10
echo "OK!"

echo "Creating 'pgobserver' database on Influxdb..."
# curl -iG 'http://localhost:8086/db/mydb/series?u=root&p=root&pretty=true' --data-urlencode "q=select * from log_lines"
curl -i -X POST -d '{\"name\": \"pgobserver\"}' 'http://localhost:8086/db?u=root&p=root'

echo "Installing Packages ..."
echo "apt-get install -y python-pip jq nginx-full"
apt-get install -y jq nginx-full vim
# apt-get install -y python-pip
# echo "pip install influxdb"
# pip install influxdb


echo "Downloading Grafana..."
echo "Determining the latest version from Github..."
echo "curl -Gs 'https://api.github.com/repos/grafana/grafana/tags'"
curl -Gs 'https://api.github.com/repos/grafana/grafana/tags' > grafana_tags.json
GRAFANA_VER=$( jq .[0].name grafana_tags.json | grep -o '[0-9].*[0-9]' )
GRAFANA_PKG=grafana-${GRAFANA_VER}.tar.gz
GRAFANA_URL=http://grafanarel.s3.amazonaws.com/$GRAFANA_PKG
echo "Downloading Grafana from $GRAFANA_URL..."
wget -nc $GRAFANA_URL
tar xvfz $GRAFANA_PKG
GRAFANA_FOLDER=grafana-${GRAFANA_VER}

cp /vagrant/grafana.config.js $GRAFANA_FOLDER/config.js

sed -i "s|/usr/share/nginx/html|/home/vagrant/$GRAFANA_FOLDER|g" /etc/nginx/sites-available/default
service nginx restart

echo ""
echo ""
echo "Grafana URL - http://0.0.0.0:8082"
echo "InxluxDB Frontend URL - http://0.0.0.0:8083"
echo "InxluxDB API URL - http://0.0.0.0:8086"
echo ""
echo ""
echo "Finished!"


