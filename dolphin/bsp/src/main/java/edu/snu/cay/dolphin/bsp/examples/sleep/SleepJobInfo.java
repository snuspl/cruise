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
package edu.snu.cay.dolphin.bsp.examples.sleep;

import edu.snu.cay.dolphin.bsp.core.DataParser;
import edu.snu.cay.dolphin.bsp.core.StageInfo;
import edu.snu.cay.dolphin.bsp.core.UserJobInfo;
import edu.snu.cay.common.metric.InsertableMetricTracker;
import edu.snu.cay.dolphin.bsp.examples.simple.SimpleDataParser;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * The {@link UserJobInfo} for SleepREEF.
 * This consists of a single stage that is optimizable.
 * Broadcast and Reduce are used to synchronize {@link SleepCtrlTask} and {@link SleepCmpTask}s.
 * The data parser is a dummy; input data is not actually used.
 */
public final class SleepJobInfo implements UserJobInfo {

  @Inject
  private SleepJobInfo() {
  }

  @Override
  public List<StageInfo> getStageInfoList() {
    final List<StageInfo> stageInfoList = new ArrayList<>(1);
    stageInfoList.add(
        StageInfo.newBuilder(SleepCmpTask.class, SleepCtrlTask.class, SleepCommGroup.class)
            .addMetricTrackers(InsertableMetricTracker.class)
            .setBroadcast(SleepCodec.class)
            .setReduce(SleepCodec.class, SleepReduceFunction.class)
            .setOptimizable(true)
            .build());
    return stageInfoList;
  }

  @Override
  public Class<? extends DataParser> getDataParser() {
    return SimpleDataParser.class;
  }
}
