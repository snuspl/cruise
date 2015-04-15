package edu.snu.reef.flexion.examples.ml.algorithms.classification;


import edu.snu.reef.flexion.core.DataParser;
import edu.snu.reef.flexion.core.StageInfo;
import edu.snu.reef.flexion.core.UserJobInfo;
import edu.snu.reef.flexion.examples.ml.data.ClassificationDataParser;
import edu.snu.reef.flexion.examples.ml.parameters.CommunicationGroup;
import edu.snu.reef.flexion.examples.ml.sub.LinearModelCodec;
import edu.snu.reef.flexion.examples.ml.sub.LogisticRegReduceFunction;
import edu.snu.reef.flexion.examples.ml.sub.LogisticRegSummaryCodec;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public final class LogisticRegJobInfo implements UserJobInfo {

  @Inject
  public LogisticRegJobInfo(){
  }

  @Override
  public List<StageInfo> getStageInfoList() {

    final List<StageInfo> stageInfoList = new LinkedList<>();

    stageInfoList.add(
        StageInfo.newBuilder(LogisticRegCmpTask.class, LogisticRegCtrlTask.class, CommunicationGroup.class)
            .setBroadcast(LinearModelCodec.class)
            .setReduce(LogisticRegSummaryCodec.class, LogisticRegReduceFunction.class)
            .build());


    return stageInfoList;
  }

  @Override
  public Class<? extends DataParser> getDataParser() {
    return ClassificationDataParser.class;
  }
}
