/*
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
package edu.snu.cay.dolphin.bsp.examples.ml.algorithms.classification;

import edu.snu.cay.common.metric.InsertableMetricTracker;
import edu.snu.cay.common.metric.TimeMetricTracker;
import edu.snu.cay.dolphin.bsp.core.DataParser;
import edu.snu.cay.dolphin.bsp.core.StageInfo;
import edu.snu.cay.dolphin.bsp.core.UserJobInfo;
import edu.snu.cay.dolphin.bsp.examples.ml.data.ClassificationDenseDataParser;
import edu.snu.cay.dolphin.bsp.examples.ml.data.ClassificationSparseDataParser;
import edu.snu.cay.dolphin.bsp.examples.ml.parameters.CommunicationGroup;
import edu.snu.cay.dolphin.bsp.examples.ml.sub.*;
import edu.snu.cay.dolphin.bsp.examples.ml.parameters.IsDenseVector;
import edu.snu.cay.common.metric.GCMetricTracker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public final class LogisticRegJobInfo implements UserJobInfo {

  private final boolean isDenseVector;

  @Inject
  public LogisticRegJobInfo(@Parameter(IsDenseVector.class) final boolean isDenseVector) {
    this.isDenseVector = isDenseVector;
  }

  @Override
  public List<StageInfo> getStageInfoList() {
    final List<StageInfo> stageInfoList = new LinkedList<>();

    stageInfoList.add(
        StageInfo.newBuilder(LogisticRegPreCmpTask.class, LogisticRegPreCtrlTask.class, LogisticRegPreCommGroup.class)
            .addMetricTrackers(InsertableMetricTracker.class, TimeMetricTracker.class, GCMetricTracker.class)
            .build());

    stageInfoList.add(
        StageInfo.newBuilder(LogisticRegMainCmpTask.class, LogisticRegMainCtrlTask.class, CommunicationGroup.class)
            .setBroadcast(isDenseVector ? DenseLinearModelCodec.class : SparseLinearModelCodec.class)
            .setReduce(isDenseVector ? DenseLogisticRegSummaryCodec.class : SparseLogisticRegSummaryCodec.class,
                LogisticRegReduceFunction.class)
            .addMetricTrackers(InsertableMetricTracker.class, TimeMetricTracker.class, GCMetricTracker.class)
            .setOptimizable(true)
            .build());

    return stageInfoList;
  }

  @Override
  public Class<? extends DataParser> getDataParser() {
    return isDenseVector ? ClassificationDenseDataParser.class : ClassificationSparseDataParser.class;
  }
}
