package edu.snu.reef.flexion.examples.ml.algorithms.regression;


import edu.snu.reef.flexion.core.DataParser;
import edu.snu.reef.flexion.core.StageInfo;
import edu.snu.reef.flexion.core.UserJobInfo;
import edu.snu.reef.flexion.examples.ml.data.ClassificationDataParser;
import edu.snu.reef.flexion.examples.ml.parameters.CommunicationGroup;
import edu.snu.reef.flexion.examples.ml.sub.LinearModelCodec;
import edu.snu.reef.flexion.examples.ml.sub.LinearRegSummaryCodec;
import edu.snu.reef.flexion.examples.ml.sub.LinearRegReduceFunction;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public final class LinearRegJobInfo implements UserJobInfo {

    @Inject
    public LinearRegJobInfo(){
    }

    @Override
    public List<StageInfo> getStageInfoList() {

        final List<StageInfo> stageInfoList = new LinkedList<>();

        stageInfoList.add(new StageInfo(LinearRegCmpTask.class, LinearRegCtrlTask.class, CommunicationGroup.class)
                .setBroadcast(LinearModelCodec.class)
                .setReduce(LinearRegSummaryCodec.class, LinearRegReduceFunction.class));

        return stageInfoList;
    }

    @Override
    public Class<? extends DataParser> getDataParser() {
        return ClassificationDataParser.class;
    }
}
