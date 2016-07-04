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
# ./run_mlr.sh -split 4 -num_servers 2 -num_partitions 4 -num_worker_threads 1 -local true -input sample_mlr -max_num_eval_local 7 -max_iter 20 -init_step_size 0.1 -classes 10 -features 784 -features_per_partition 392 -model_gaussian 0.001 -num_batch_per_iter 3 -lambda 0.005 -timeout 200000 -status_log_period 5 -decay_period 5 -decay_rate 0.9 -num_batch_per_loss_log 1 -train_error_dataset_size 30 -dynamic false -optimizer edu.snu.cay.services.em.optimizer.impl.EmptyPlanOptimizer -plan_executor edu.snu.cay.dolphin.async.optimizer.AsyncDolphinPlanExecutor -optimization_interval_ms 3000 -memory_store_init_delay_ms 1000 -delay_after_optimization_ms 10000

SELF_JAR=`echo ../target/dolphin-async-*-shaded.jar`

LOGGING_CONFIG='-Djava.util.logging.config.class=edu.snu.cay.utils.LoggingConfig'

CLASSPATH=$YARN_HOME/share/hadoop/common/*:$YARN_HOME/share/hadoop/common/lib/*:$YARN_HOME/share/hadoop/yarn/*:$YARN_HOME/share/hadoop/hdfs/*:$YARN_HOME/share/hadoop/mapreduce/lib/*:$YARN_HOME/share/hadoop/mapreduce/*

YARN_CONF_DIR=$YARN_HOME/etc/hadoop

ALG=edu.snu.cay.dolphin.async.mlapps.mlr.MLRREEF

CMD="java -cp $YARN_CONF_DIR:$SELF_JAR:$CLASSPATH $LOGGING_CONFIG $ALG $*"
echo $CMD
$CMD
