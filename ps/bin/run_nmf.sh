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
# ./run_nmf.sh -max_num_epochs 50 -local true -number_servers 1 -num_workers 4 -max_num_eval_local 5 -input sample_nmf -num_mini_batches 25 -num_worker_blocks 25 -rank 30 -step_size 0.01 -print_mat true -timeout 300000 -decay_period 5 -decay_rate 0.9 -optimizer edu.snu.spl.cruise.ps.optimizer.impl.EmptyPlanOptimizer -optimization_interval_ms 3000 -delay_after_optimization_ms 10000 -opt_benefit_threshold 0.1 -server_metric_flush_period_ms 1000 -moving_avg_window_size 0 -metric_weight_factor 0.0 -num_initial_batch_metrics_to_skip 3 -min_num_required_batch_metrics 3 -hyper_thread_enabled false

SELF_JAR=`echo ../target/ps-*-shaded.jar`

LOGGING_CONFIG='-Djava.util.logging.config.class=edu.snu.spl.cruise.utils.LoggingConfig'

CLASSPATH=$YARN_HOME/share/hadoop/common/*:$YARN_HOME/share/hadoop/common/lib/*:$YARN_HOME/share/hadoop/yarn/*:$YARN_HOME/share/hadoop/hdfs/*:$YARN_HOME/share/hadoop/mapreduce/lib/*:$YARN_HOME/share/hadoop/mapreduce/*

YARN_CONF_DIR=$YARN_HOME/etc/hadoop

ALG=edu.snu.spl.cruise.ps.mlapps.nmf.NMFET

CMD="java -cp $YARN_CONF_DIR:$SELF_JAR:$CLASSPATH $LOGGING_CONFIG $ALG $*"
echo $CMD
$CMD
