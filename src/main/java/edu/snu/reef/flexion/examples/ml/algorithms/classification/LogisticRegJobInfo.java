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
