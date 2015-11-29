#!/bin/sh
# Copyright (C) 2015 Seoul National University
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
# ./run_sleep.sh -local true -split 3 -input sample -conf sample_sleep_conf -output output -gcSerializedObject 1 -gcEncodeRate 2 -gcDecodeRate 3 -emSerializedObject 4 -emEncodeRate 5 -emDecodeRate 6 -maxIter 10 -timeout 120000 -maxNumEvalLocal 5 -optimizer edu.snu.cay.services.em.optimizer.impl.AddOneOptimizer -plan_executor edu.snu.cay.dolphin.core.optimizer.DefaultPlanExecutor

# RUNTIME
SELF_JAR=../target/dolphin-0.1-SNAPSHOT-shaded.jar

LOGGING_CONFIG='-Djava.util.logging.config.class=org.apache.reef.util.logging.Config'

CLASSPATH=$YARN_HOME/share/hadoop/common/*:$YARN_HOME/share/hadoop/common/lib/*:$YARN_HOME/share/hadoop/yarn/*:$YARN_HOME/share/hadoop/hdfs/*:$YARN_HOME/share/hadoop/mapreduce/lib/*:$YARN_HOME/share/hadoop/mapreduce/*

YARN_CONF_DIR=$YARN_HOME/etc/hadoop

ALG=edu.snu.cay.dolphin.examples.sleep.SleepREEF

CMD="java -cp $YARN_CONF_DIR:$SELF_JAR:$CLASSPATH $LOCAL_RUNTIME_TMP $LOGGING_CONFIG $ALG $*"
echo $CMD
$CMD # 2> /dev/null
