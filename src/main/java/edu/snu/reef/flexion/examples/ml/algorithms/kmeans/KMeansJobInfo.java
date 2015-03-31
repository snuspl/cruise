package edu.snu.reef.flexion.examples.ml.algorithms.kmeans;


import edu.snu.reef.flexion.core.DataParser;
import edu.snu.reef.flexion.core.UserJobInfo;
import edu.snu.reef.flexion.core.UserTaskInfo;
import edu.snu.reef.flexion.examples.ml.sub.CentroidListCodec;
import edu.snu.reef.flexion.examples.ml.sub.MapOfIntVSumCodec;
import edu.snu.reef.flexion.examples.ml.sub.MapOfIntVSumReduceFunction;
import edu.snu.reef.flexion.examples.ml.sub.VectorListCodec;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public final class KMeansJobInfo implements UserJobInfo {

    @Inject
    public KMeansJobInfo(){
    }

    @Override
    public List<UserTaskInfo> getTaskInfoList() {

        List<UserTaskInfo> taskInfoList = new LinkedList<>();

        // preprocess: initialize the centroids of clusters
        taskInfoList.add(new UserTaskInfo(KMeansPreCmpTask.class, KMeansPreCtrlTask.class, KMeansPreCommGroup.class)
                .setGather(VectorListCodec.class));

        // main process: adjust the centroids of clusters
        taskInfoList.add(new UserTaskInfo(KMeansMainCmpTask.class, KMeansMainCtrlTask.class, KMeansMainCommGroup.class)
                .setBroadcast(CentroidListCodec.class)
                .setReduce(MapOfIntVSumCodec.class, MapOfIntVSumReduceFunction.class));

        return taskInfoList;
    }

    @Override
    public Class<? extends DataParser> getDataParser() {
        return KMeansDataParser.class;
    }
}
