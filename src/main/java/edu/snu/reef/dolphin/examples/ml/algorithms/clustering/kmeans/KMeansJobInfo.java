/**
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
package edu.snu.reef.dolphin.examples.ml.algorithms.clustering.kmeans;

import edu.snu.reef.dolphin.core.DataParser;
import edu.snu.reef.dolphin.core.StageInfo;
import edu.snu.reef.dolphin.core.UserJobInfo;
import edu.snu.reef.dolphin.examples.ml.algorithms.clustering.ClusteringPreStageBuilder;
import edu.snu.reef.dolphin.examples.ml.data.ClusteringDataParser;
import edu.snu.reef.dolphin.examples.ml.sub.CentroidListCodec;
import edu.snu.reef.dolphin.examples.ml.sub.MapOfIntVSumCodec;
import edu.snu.reef.dolphin.examples.ml.sub.MapOfIntVSumReduceFunction;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public final class KMeansJobInfo implements UserJobInfo {

  @Inject
  public KMeansJobInfo(){
  }

  @Override
  public List<StageInfo> getStageInfoList() {
    final List<StageInfo> stageInfoList = new LinkedList<>();

    // preprocess: initialize the centroids of clusters
    stageInfoList.add(ClusteringPreStageBuilder.build());

    // main process: adjust the centroids of clusters
    stageInfoList.add(
        StageInfo.newBuilder(KMeansMainCmpTask.class, KMeansMainCtrlTask.class, KMeansMainCommGroup.class)
            .setBroadcast(CentroidListCodec.class)
            .setReduce(MapOfIntVSumCodec.class, MapOfIntVSumReduceFunction.class)
            .build());

    return stageInfoList;
  }

  @Override
  public Class<? extends DataParser> getDataParser() {
    return ClusteringDataParser.class;
  }
}
