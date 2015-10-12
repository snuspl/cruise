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
package edu.snu.cay.dolphin.core.optimizer;

import edu.snu.cay.dolphin.core.ComputeTask;
import edu.snu.cay.dolphin.core.ControllerTask;
import edu.snu.cay.dolphin.core.DolphinMetricKeys;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;

import java.util.*;

/**
 * Class for calculating costs of Dolphin.
 * Assumes that only one controller task exists.
 */
final class CostCalculator {

  private final EvaluatorParameters controlTaskParameters;
  private final List<EvaluatorParameters> computeTasksParameters;

  private CostCalculator(final Collection<EvaluatorParameters> activeEvaluators) {
    computeTasksParameters = new ArrayList<>();
    EvaluatorParameters tempControlTaskParameters = null;

    for (final EvaluatorParameters evaluatorParameters : activeEvaluators) {
      if (evaluatorParameters.getId().startsWith(ControllerTask.TASK_ID_PREFIX)) {
        tempControlTaskParameters = evaluatorParameters;
      } else if (evaluatorParameters.getId().startsWith(ComputeTask.TASK_ID_PREFIX)) {
        computeTasksParameters.add(evaluatorParameters);
      }
    }

    if (tempControlTaskParameters != null) {
      controlTaskParameters = tempControlTaskParameters;
    } else {
      throw new RuntimeException("There is no controller task among active evaluators");
    }
  }

  public static CostCalculator newInstance(final Collection<EvaluatorParameters> activeEvaluators) {
    return new CostCalculator(activeEvaluators);
  }

  /**
   * Calculates costs of Dolphin.
   * @return the calculated costs
   */
  public Cost calculate() {
    final Collection<Cost.ComputeTaskCost> computeTaskCosts = getComputeTaskCosts();
    final double communicationCost = getCommunicationCost(computeTaskCosts);
    return new Cost(communicationCost, computeTaskCosts);
  }

  /**
   * @return a collection of costs for {@link ComputeTask}.
   */
  private Collection<Cost.ComputeTaskCost> getComputeTaskCosts() {
    final Collection<Cost.ComputeTaskCost> ret = new ArrayList<>();
    for (final EvaluatorParameters evaluatorParameters : computeTasksParameters) {
      ret.add(getComputeTaskCost(evaluatorParameters));
    }
    return ret;
  }

  /**
   * Generates {@link edu.snu.cay.dolphin.core.optimizer.Cost.ComputeTaskCost} by calculating compute cost and using
   * this metadata.
   * Assumes that the specified evaluator parameters object is for {@link ComputeTask}.
   * @param evaluatorParameters the evaluator parameters object for {@link ComputeTask}.
   * @return The generated compute task cost.
   */
  private Cost.ComputeTaskCost getComputeTaskCost(final EvaluatorParameters evaluatorParameters) {
    final Map<String, Double> metrics = evaluatorParameters.getMetrics();
    final double cmpCost = metrics.get(DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_END) -
        metrics.get(DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_START);
    return new Cost.ComputeTaskCost(evaluatorParameters.getId(), cmpCost, evaluatorParameters.getDataInfos());
  }

  /**
   * Class for comparing two {@link edu.snu.cay.dolphin.core.optimizer.Cost.ComputeTaskCost} by compute costs.
   */
  private class CmpCostComparator implements Comparator<Cost.ComputeTaskCost> {
    @Override
    public int compare(final Cost.ComputeTaskCost o1, final Cost.ComputeTaskCost o2) {
      return Double.compare(o1.getComputeCost(), o2.getComputeCost());
    }
  }

  /**
   * Computes a communication cost for the previous execution.
   * The communication cost is calculated by subtracting the maximum compute cost among {@link ComputeTask}s
   * from the elapsed time between when the controller task started to broadcast and when it finished reduce operation.
   * @param cmpTaskCosts a collection of costs for {@link ComputeTask}.
   * @return the calculated communication cost.
   */
  private double getCommunicationCost(final Collection<Cost.ComputeTaskCost> cmpTaskCosts) {
    // find the maximum compute cost for every compute task.
    final double maxCmpCost = Collections.max(cmpTaskCosts, new CmpCostComparator()).getComputeCost();

    // (communication cost) = (reduce end time) - (broadcast start time) - max(compute cost for each compute task)
    return controlTaskParameters.getMetrics().get(DolphinMetricKeys.CONTROLLER_TASK_RECEIVE_DATA_END)
        - controlTaskParameters.getMetrics().get(DolphinMetricKeys.CONTROLLER_TASK_SEND_DATA_START)
        - maxCmpCost;
  }
}
