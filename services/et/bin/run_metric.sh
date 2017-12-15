#!/bin/sh
# Copyright (C) 2017 Seoul National University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# EXAMPLE USAGE 
# ./run_metric.sh -metric_automatic_flush_period_ms 5000 -metric_manual_flush_period_ms 2000 -custom_metric_record_period_ms 100 -task_duration_ms 20000

# RUNTIME
SELF_JAR=`echo ../target/elastic-tables-*-SNAPSHOT-shaded.jar`

LOGGING_CONFIG='-Djava.util.logging.config.class=edu.snu.spl.cruise.utils.LoggingConfig'

CLASSPATH=$YARN_HOME/share/hadoop/common/*:$YARN_HOME/share/hadoop/common/lib/*:$YARN_HOME/share/hadoop/yarn/*:$YARN_HOME/share/hadoop/hdfs/*:$YARN_HOME/share/hadoop/mapreduce/lib/*:$YARN_HOME/share/hadoop/mapreduce/*

YARN_CONF_DIR=$YARN_HOME/etc/hadoop

JOB='edu.snu.spl.cruise.services.et.examples.metric.MetricET'

CMD="java -cp $YARN_CONF_DIR:$SELF_JAR:$CLASSPATH $LOCAL_RUNTIME_TMP $LOGGING_CONFIG $JOB $*"
echo $CMD
$CMD # 2> /dev/null
