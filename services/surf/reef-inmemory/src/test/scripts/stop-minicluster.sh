#!/usr/bin/env bash

PROCESS=`ps -ef | grep minicluster | grep -v $0 | grep -v grep`
echo "Found processes:"
echo $PROCESS
PID=`echo $PROCESS | awk '{print $2}'`
for pid in $PID; do
  echo "Killing: $pid"
  kill -9 $pid
done
