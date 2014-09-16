#!/usr/bin/env bash

NNPORT=$1
echo "Starting minicluster"
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar minicluster \
  -Ddfs.namenode.fs-limits.min-block-size=512 -nomr -nnport $NNPORT -datanodes 3 &
echo "Sleep 10..."
sleep 10