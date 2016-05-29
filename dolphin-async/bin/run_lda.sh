#!/bin/sh
# Copyright (C) 2016 Seoul National University
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
# ./run_lda.sh -input sample_lda -local true -split 6 -numServers 2 -numTopics 10 -numVocabs 102656 -maxIter 1 -maxNumEvalLocal 8 -numWorkerThreads 1 -timeout 300000 -dynamic false -optimizer edu.snu.cay.services.em.optimizer.impl.EmptyPlanOptimizer -plan_executor edu.snu.cay.async.optimizer.AsyncDolphinPlanExecutor -optimizationIntervalMs 3000 -memoryStoreInitDelayMs 1000 -delayAfterOptimizationMs 10000

SELF_JAR=`echo ../target/dolphin-async-*-shaded.jar`

LOGGING_CONFIG='-Djava.util.logging.config.class=edu.snu.cay.utils.LoggingConfig'

CLASSPATH=$YARN_HOME/share/hadoop/common/*:$YARN_HOME/share/hadoop/common/lib/*:$YARN_HOME/share/hadoop/yarn/*:$YARN_HOME/share/hadoop/hdfs/*:$YARN_HOME/share/hadoop/mapreduce/lib/*:$YARN_HOME/share/hadoop/mapreduce/*

YARN_CONF_DIR=$YARN_HOME/etc/hadoop

ALG=edu.snu.cay.async.examples.lda.LdaREEF

CMD="java -cp $YARN_CONF_DIR:$SELF_JAR:$CLASSPATH $LOGGING_CONFIG $ALG $*"
echo $CMD
$CMD
