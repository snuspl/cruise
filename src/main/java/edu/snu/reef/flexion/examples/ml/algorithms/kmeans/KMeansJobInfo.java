package edu.snu.reef.flexion.examples.ml.algorithms.kmeans;


import edu.snu.reef.flexion.core.*;
import edu.snu.reef.flexion.examples.ml.sub.CentroidListCodec;
import edu.snu.reef.flexion.examples.ml.sub.MapOfIntVSumCodec;
import edu.snu.reef.flexion.examples.ml.sub.MapOfIntVSumReduceFunction;
import edu.snu.reef.flexion.examples.ml.sub.VectorListCodec;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public class KMeansJobInfo implements UserJobInfo {

    @Inject
    public KMeansJobInfo(){
    }

    @Override
    public List<UserTaskInfo> getTaskInfoList() {

        List<UserTaskInfo> taskInfoList = new LinkedList<>();
        taskInfoList.add(new UserTaskInfo(KMeansCmpTask.class, KMeansCtrlTask.class, KMeansCommGroup.class)
                .setBroadcast(CentroidListCodec.class)
                .setGather(VectorListCodec.class)
                .setReduce(MapOfIntVSumCodec.class, MapOfIntVSumReduceFunction.class));
        return taskInfoList;
    }

    @Override
    public Class<? extends DataParser> getDataParser() {
        return KMeansDataParser.class;
    }
}
