#!/bin/sh

basedir=$(dirname "$0")
nohup python2.7 "${basedir}/src/web.py" "$@" > "${basedir}/pgobserver_web.log" &
