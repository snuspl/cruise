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
package edu.snu.cay.dolphin.bsp.mlapps.algorithms.clustering;

import edu.snu.cay.dolphin.bsp.core.StageInfo;
import edu.snu.cay.dolphin.bsp.mlapps.sub.VectorListCodec;
import edu.snu.cay.common.metric.GCMetricTracker;
import edu.snu.cay.common.metric.InsertableMetricTracker;
import edu.snu.cay.common.metric.TimeMetricTracker;

public final class ClusteringPreStageBuilder {
  /**
   * Should not be instantiated.
   */
  private ClusteringPreStageBuilder() {
  }

  public static StageInfo build() {
    return StageInfo.newBuilder(ClusteringPreCmpTask.class, ClusteringPreCtrlTask.class, ClusteringPreCommGroup.class)
        .setGather(VectorListCodec.class)
        .addMetricTrackers(InsertableMetricTracker.class, TimeMetricTracker.class, GCMetricTracker.class)
        .build();
  }
}
