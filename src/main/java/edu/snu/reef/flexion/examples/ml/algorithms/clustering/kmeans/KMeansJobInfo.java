package edu.snu.reef.flexion.examples.ml.algorithms.clustering.kmeans;


import edu.snu.reef.flexion.core.DataParser;
import edu.snu.reef.flexion.core.StageInfo;
import edu.snu.reef.flexion.core.UserJobInfo;
import edu.snu.reef.flexion.examples.ml.algorithms.clustering.ClusteringPreStageBuilder;
import edu.snu.reef.flexion.examples.ml.data.ClusteringDataParser;
import edu.snu.reef.flexion.examples.ml.sub.CentroidListCodec;
import edu.snu.reef.flexion.examples.ml.sub.MapOfIntVSumCodec;
import edu.snu.reef.flexion.examples.ml.sub.MapOfIntVSumReduceFunction;

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
