#!/bin/bash

export PGHOST=localhost
export PGUSER=postgres
export PGPASSWORD=postgres
export PGDATABASE=local_pgobserver_db

container=$(docker ps | grep postgres:9.3.5)
if [ -z "$container" ]; then
    docker rm postgres
    docker run --name postgres --net host -e POSTGRES_PASSWORD=postgres -d postgres:9.3.5
fi

until nc -w 5 -z localhost 5432; do
    echo 'Waiting for Postgres port..'
    sleep 3
done

cd /vagrant/sql/schema
psql -c "CREATE DATABASE $PGDATABASE;" postgres
find -name '*.sql' | sort | xargs cat | psql

psql -d "$PGDATABASE" -c "INSERT INTO hosts (host_name, host_user, host_password, host_db, host_ui_shortname, host_ui_longname) VALUES ('${PGHOST}', '${PGUSER}', '${PGPASSWORD}', '${PGDATABASE}', 'pgo', 'PgoMetricsDB')"
psql -d "$PGDATABASE" -c "UPDATE hosts SET host_settings = '{\"statDatabaseGatherInterval\": 1, \"tableStatsGatherInterval\": 1, \"tableIoGatherInterval\": 1, \"sprocGatherInterval\": 1}' WHERE host_id = 1 "


for comp in gatherer frontend; do
    docker rm pgobserver-$comp
    docker run --name pgobserver-$comp --net host -d pgobserver-$comp
done

echo ""
echo ""
echo "*** Setup FINISHED ***"
echo "Frontend URL: http://localhost:38080    (to get some graphs shown you need to wait some minutes for metrics deltas to be captured)"
echo ""
echo "FYI - currently only the metrics datastore itself is under monitoring, to add more servers to be monitored:"
echo " 1) Go to http://localhost:38080/hosts to configure connection strings and monitored metrics for hosts you want to monitor"
echo " 2) Go to the metrics DB and set username/password (not visible in UI for security reasons) for configured hosts"
echo " 3) Log into Vagrant (vagrant ssh) and restart the gatherer docker image with:"
echo "    - 'docker rm -f pgobserver-gatherer && docker run --name pgobserver-gatherer --net host -d pgobserver-gatherer'"
echo "    - or wait 10min (interval for configuration changes checker)"
