package edu.snu.reef.flexion.examples.simple;


import edu.snu.reef.flexion.core.*;
import org.apache.reef.io.serialization.SerializableCodec;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public class SimpleJobInfo implements UserJobInfo{

  @Inject
  public SimpleJobInfo(){
  }

  @Override
  public List<StageInfo> getStageInfoList() {
    final List<StageInfo> stageInfoList = new LinkedList<>();
    stageInfoList.add(
        StageInfo.newBuilder(SimpleCmpTask.class, SimpleCtrlTask.class, SimpleCommGroup.class)
            .setBroadcast(SerializableCodec.class)
            .setReduce(SerializableCodec.class, SimpleReduceFunction.class)
            .build());
    return stageInfoList;
  }

  @Override
  public Class<? extends DataParser> getDataParser() {
    return SimpleDataParser.class;
  }
}
