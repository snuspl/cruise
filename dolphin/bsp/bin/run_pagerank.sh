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
# ./run_pagerank.sh -conv_thr 0.01 -max_iter 10 -damping_factor 0.85 -local true -split 1 -input sample_pagerank -output output_pagerank -max_num_eval_local 2

# RUNTIME
SELF_JAR=../target/dolphin-bsp-0.1-SNAPSHOT-shaded.jar

LOGGING_CONFIG='-Djava.util.logging.config.class=edu.snu.cay.utils.LoggingConfig'

CLASSPATH=$YARN_HOME/share/hadoop/common/*:$YARN_HOME/share/hadoop/common/lib/*:$YARN_HOME/share/hadoop/yarn/*:$YARN_HOME/share/hadoop/hdfs/*:$YARN_HOME/share/hadoop/mapreduce/lib/*:$YARN_HOME/share/hadoop/mapreduce/*

YARN_CONF_DIR=$YARN_HOME/etc/hadoop

ALG=edu.snu.cay.dolphin.bsp.mlapps.algorithms.graph.PageRankREEF

CMD="java -cp $YARN_CONF_DIR:$SELF_JAR:$CLASSPATH $LOCAL_RUNTIME_TMP $LOGGING_CONFIG $ALG $*"
echo $CMD
$CMD # 2> /dev/null
