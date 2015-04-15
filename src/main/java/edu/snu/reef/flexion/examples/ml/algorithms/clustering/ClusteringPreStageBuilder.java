package edu.snu.reef.flexion.examples.ml.algorithms.clustering;

import edu.snu.reef.flexion.core.StageInfo;
import edu.snu.reef.flexion.examples.ml.sub.VectorListCodec;

public class ClusteringPreStageBuilder {

  public static StageInfo build() {
    return StageInfo.newBuilder(ClusteringPreCmpTask.class, ClusteringPreCtrlTask.class, ClusteringPreCommGroup.class)
        .setGather(VectorListCodec.class).build();
  }
}
