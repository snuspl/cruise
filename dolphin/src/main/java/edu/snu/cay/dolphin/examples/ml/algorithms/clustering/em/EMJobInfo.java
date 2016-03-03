/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.dolphin.examples.ml.algorithms.clustering.em;

import edu.snu.cay.common.metric.InsertableMetricTracker;
import edu.snu.cay.common.metric.TimeMetricTracker;
import edu.snu.cay.dolphin.core.DataParser;
import edu.snu.cay.dolphin.core.StageInfo;
import edu.snu.cay.dolphin.core.UserJobInfo;
import edu.snu.cay.common.metric.GCMetricTracker;
import edu.snu.cay.dolphin.examples.ml.algorithms.clustering.ClusteringPreStageBuilder;
import edu.snu.cay.dolphin.examples.ml.data.ClusteringDataParser;
import edu.snu.cay.dolphin.examples.ml.sub.ClusterSummaryListCodec;
import edu.snu.cay.dolphin.examples.ml.sub.MapOfIntClusterStatsCodec;
import edu.snu.cay.dolphin.examples.ml.sub.MapOfIntClusterStatsReduceFunction;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public final class EMJobInfo implements UserJobInfo {

  @Inject
  public EMJobInfo() {
  }

  @Override
  public List<StageInfo> getStageInfoList() {
    final List<StageInfo> stageInfoList = new LinkedList<>();

    // preprocess: initialize the centroids of clusters
    stageInfoList.add(ClusteringPreStageBuilder.build());

    // main process: adjust the centroids and covariance matrices of clusters
    stageInfoList.add(
        StageInfo.newBuilder(EMMainCmpTask.class, EMMainCtrlTask.class, EMMainCommGroup.class)
            .setBroadcast(ClusterSummaryListCodec.class)
            .setReduce(MapOfIntClusterStatsCodec.class, MapOfIntClusterStatsReduceFunction.class)
            .addMetricTrackers(InsertableMetricTracker.class, TimeMetricTracker.class, GCMetricTracker.class)
            .setOptimizable(true)
            .build());

    return stageInfoList;
  }

  @Override
  public Class<? extends DataParser> getDataParser() {
    return ClusteringDataParser.class;
  }
}
