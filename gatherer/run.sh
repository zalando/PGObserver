#!/bin/bash
DATE=$(date +%F_%H%M)
nohup java -jar PGObserver-Gatherer-1.0-jar-with-dependencies.jar &> pgmon_java_${DATE}.log &
