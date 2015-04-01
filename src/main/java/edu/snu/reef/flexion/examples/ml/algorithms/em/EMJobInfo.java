package edu.snu.reef.flexion.examples.ml.algorithms.em;


import edu.snu.reef.flexion.core.DataParser;
import edu.snu.reef.flexion.core.StageInfo;
import edu.snu.reef.flexion.core.UserJobInfo;
import edu.snu.reef.flexion.examples.ml.algorithms.ClusteringDataParser;
import edu.snu.reef.flexion.examples.ml.algorithms.ClusteringPreCmpTask;
import edu.snu.reef.flexion.examples.ml.algorithms.ClusteringPreCommGroup;
import edu.snu.reef.flexion.examples.ml.algorithms.ClusteringPreCtrlTask;
import edu.snu.reef.flexion.examples.ml.sub.*;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public final class EMJobInfo implements UserJobInfo {

    @Inject
    public EMJobInfo(){
    }

    @Override
    public List<StageInfo> getStageInfoList() {

        List<StageInfo> stageInfoList = new LinkedList<>();

        // preprocess: initialize the centroids of clusters
        stageInfoList.add(new StageInfo(ClusteringPreCmpTask.class, ClusteringPreCtrlTask.class, ClusteringPreCommGroup.class)
                .setGather(VectorListCodec.class));

        // main process: adjust the centroids and covariance matrices of clusters
        stageInfoList.add(new StageInfo(EMMainCmpTask.class, EMMainCtrlTask.class, EMMainCommGroup.class)
                .setBroadcast(ClusterSummaryListCodec.class)
                .setReduce(MapOfIntClusterStatsCodec.class, MapOfIntClusterStatsReduceFunction.class));

        return stageInfoList;
    }

    @Override
    public Class<? extends DataParser> getDataParser() {
        return ClusteringDataParser.class;
    }
}
