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
# Regression (both real-number features and categorical features)
# ./run_gbt.sh -split 2 -max_num_epochs 300 -mini_batch_size 5000 -features 30 -max_num_eval_local 3 -input sample_gbt_student_score -local true -eval_size 1024 -timeout 200000 -dynamic false -optimizer edu.snu.cay.services.em.optimizer.impl.EmptyPlanOptimizer -plan_executor edu.snu.cay.dolphin.async.plan.AsyncDolphinPlanExecutor -optimization_interval_ms 3000 -delay_after_optimization_ms 10000 -worker_log_period_ms 0 -server_log_period_ms 0 -server_metrics_window_ms 1000 -init_step_size 0.05 -lambda 0.5 -gamma 0.5 -max_depth_of_tree 8 -leaf_min_size 3 -metadata_path file://$(PWD)/sample_gbt_student_score.meta -num_keys_in_server 5
# Classification (only real-number features)
# ./run_gbt.sh -split 2 -max_num_epochs 50 -mini_batch_size 5000 -features 27 -max_num_eval_local 3 -input sample_gbt_forest -local true -eval_size 1024 -timeout 200000 -dynamic false -optimizer edu.snu.cay.services.em.optimizer.impl.EmptyPlanOptimizer -plan_executor edu.snu.cay.dolphin.async.plan.AsyncDolphinPlanExecutor -optimization_interval_ms 3000 -delay_after_optimization_ms 10000 -worker_log_period_ms 0 -server_log_period_ms 0 -server_metrics_window_ms 1000 -init_step_size 0.1 -lambda 0.5 -gamma 0.5 -max_depth_of_tree 8 -leaf_min_size 3 -metadata_path file://$(PWD)/sample_gbt_forest.meta -num_keys_in_server 5
# Classification (only categorical features)
# ./run_gbt.sh -split 4 -max_num_epochs 50 -mini_batch_size 5000 -features 19 -max_num_eval_local 5 -input sample_gbt_mushroom -local true -eval_size 1024 -timeout 200000 -dynamic false -optimizer edu.snu.cay.services.em.optimizer.impl.EmptyPlanOptimizer -plan_executor edu.snu.cay.dolphin.async.plan.AsyncDolphinPlanExecutor -optimization_interval_ms 3000 -delay_after_optimization_ms 10000 -worker_log_period_ms 0 -server_log_period_ms 0 -server_metrics_window_ms 1000 -init_step_size 0.1 -lambda 0.1 -gamma 0.1 -max_depth_of_tree 8 -leaf_min_size 3 -metadata_path file://$(PWD)/sample_gbt_mushroom.meta -num_keys_in_server 5


SELF_JAR=`echo ../target/dolphin-async-*-shaded.jar`

LOGGING_CONFIG='-Djava.util.logging.config.class=edu.snu.cay.utils.LoggingConfig'

CLASSPATH=$YARN_HOME/share/hadoop/common/*:$YARN_HOME/share/hadoop/common/lib/*:$YARN_HOME/share/hadoop/yarn/*:$YARN_HOME/share/hadoop/hdfs/*:$YARN_HOME/share/hadoop/mapreduce/lib/*:$YARN_HOME/share/hadoop/mapreduce/*

YARN_CONF_DIR=$YARN_HOME/etc/hadoop

ALG=edu.snu.cay.dolphin.async.mlapps.gbt.GBTREEF

CMD="java -cp $YARN_CONF_DIR:$SELF_JAR:$CLASSPATH $LOGGING_CONFIG $ALG $*"
echo $CMD
$CMD
