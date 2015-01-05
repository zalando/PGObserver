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
psql -c 'CREATE EXTENSION hstore;'
find -name '*.sql' | sort | xargs cat | psql
psql -f /vagrant/vagrant/initial.sql

ip=$(ip -o -4 a show eth0|awk '{print $4}' | cut -d/ -f 1)

for comp in gatherer frontend; do
    docker rm pgobserver-$comp
    docker run --name pgobserver-$comp --net host -d pgobserver-$comp
done