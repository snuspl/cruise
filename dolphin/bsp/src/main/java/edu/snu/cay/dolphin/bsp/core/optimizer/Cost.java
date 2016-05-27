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
package edu.snu.cay.dolphin.bsp.core.optimizer;

import edu.snu.cay.services.em.optimizer.api.DataInfo;

import java.util.Collection;

/**
 * Class for storing costs of Dolphin.
 */
final class Cost {

  private final double communicationCost;
  private final Collection<ComputeTaskCost> computeTaskCosts;

  Cost(final double communicationCost, final Collection<ComputeTaskCost> computeTaskCosts) {
    this.communicationCost = communicationCost;
    this.computeTaskCosts = computeTaskCosts;
  }

  /**
   * Class for storing costs for {@link edu.snu.cay.dolphin.bsp.core.ComputeTask}s and their meta-information.
   */
  public static class ComputeTaskCost {

    private final String id;
    private final double cmpCost;
    private final Collection<DataInfo> dataInfos;

    public ComputeTaskCost(final String id, final double cmpCost, final Collection<DataInfo> datainfos) {
      this.id = id;
      this.cmpCost = cmpCost;
      this.dataInfos = datainfos;
    }

    public String getId() {
      return id;
    }

    public double getComputeCost() {
      return cmpCost;
    }

    public final Collection<DataInfo> getDataInfos() {
      return dataInfos;
    }

    @Override
    public String toString() {
      return String.format("ComputeTaskCost{id=\"%s\", cmpCost=%f, dataInfos=%s}", id, cmpCost, dataInfos);
    }
  }

  public double getCommunicationCost() {
    return communicationCost;
  }

  public Collection<ComputeTaskCost> getComputeTaskCosts() {
    return computeTaskCosts;
  }

  @Override
  public String toString() {
    return String.format("Cost{commCost=%f, computeTaskCosts=%s}", communicationCost, computeTaskCosts);
  }
}
