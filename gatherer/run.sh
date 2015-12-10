#!/bin/bash

basedir=$(dirname "$0")

DATE=$(date +%F_%H%M)
JAR=$(ls ${basedir}/target/pgobserver-gatherer-*-jar-with-dependencies.jar | tail -1)

nohup java -jar ${JAR} "$@" &> "${basedir}/pgmon_java_${DATE}.log" &
