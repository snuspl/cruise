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
package edu.snu.cay.dolphin.examples.matmul;

import edu.snu.cay.dolphin.core.DataParser;
import edu.snu.cay.dolphin.core.StageInfo;
import edu.snu.cay.dolphin.core.UserJobInfo;
import edu.snu.cay.common.metric.GCMetricTracker;
import edu.snu.cay.common.metric.InsertableMetricTracker;
import edu.snu.cay.common.metric.TimeMetricTracker;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public final class MatrixMulJobInfo implements UserJobInfo {

  @Inject
  private MatrixMulJobInfo() {
  }

  @Override
  public List<StageInfo> getStageInfoList() {
    final List<StageInfo> stageInfoList = new LinkedList<>();
    stageInfoList.add(
        StageInfo.newBuilder(MatrixMulCmpTask.class, MatrixMulCtrlTask.class, MatrixMulCommGroup.class)
            .setPreRunShuffle(IntegerCodec.class, IndexedElementCodec.class)
            .setReduce(IndexedVectorListCodec.class, MatrixMulReduceFunction.class)
            .addMetricTrackers(InsertableMetricTracker.class, TimeMetricTracker.class, GCMetricTracker.class)
            .build()
    );

    return stageInfoList;
  }

  @Override
  public Class<? extends DataParser> getDataParser() {
    return MatrixMulDataParser.class;
  }
}
