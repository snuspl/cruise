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

import edu.snu.cay.dolphin.core.DolphinMetricKeys;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;

import java.util.*;

/**
 * Class for calculating costs of Dolphin.
 * Assumes that only one controller task exists.
 */
final class CostCalculator {

  private CostCalculator() {
  }

  /**
   * Calculates costs of Dolphin.
   * @return the calculated costs
   */
  public static Cost calculate(final Collection<EvaluatorParameters> activeEvaluators,
                               final Map<String, Double> ctrlTaskMetrics) {
    final DolphinTaskParameters dolphinTaskParams =
        new DolphinTaskParameters(ctrlTaskMetrics, activeEvaluators);
    final Collection<Cost.ComputeTaskCost> computeTaskCosts =
        getComputeTaskCosts(dolphinTaskParams.getComputeTasksParameters());
    final double communicationCost =
        getCommunicationCost(dolphinTaskParams.getControllerTaskMetrics(), computeTaskCosts);
    return new Cost(communicationCost, computeTaskCosts);
  }

  /**
   * @param computeTasksParameters a collection of evaluator parameters for compute tasks
   * @return a collection of costs for {@link edu.snu.cay.dolphin.core.ComputeTask}s
   */
  private static Collection<Cost.ComputeTaskCost> getComputeTaskCosts(
      final Collection<EvaluatorParameters> computeTasksParameters) {
    final Collection<Cost.ComputeTaskCost> ret = new ArrayList<>(computeTasksParameters.size());
    for (final EvaluatorParameters evaluatorParameters : computeTasksParameters) {
      ret.add(getComputeTaskCost(evaluatorParameters));
    }
    return ret;
  }

  /**
   * Generates {@link edu.snu.cay.dolphin.core.optimizer.Cost.ComputeTaskCost} by calculating compute cost and using
   * this metadata.
   * Assumes that the specified evaluator parameters object is for {@link edu.snu.cay.dolphin.core.ComputeTask}.
   * @param evaluatorParameters the evaluator parameters object for {@link edu.snu.cay.dolphin.core.ComputeTask}.
   * @return The generated compute task cost.
   */
  private static Cost.ComputeTaskCost getComputeTaskCost(final EvaluatorParameters evaluatorParameters) {
    final Map<String, Double> metrics = evaluatorParameters.getMetrics();
    final double cmpCost = metrics.get(DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_END) -
        metrics.get(DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_START);
    return new Cost.ComputeTaskCost(evaluatorParameters.getId(), cmpCost, evaluatorParameters.getDataInfos());
  }

  /**
   * Class for comparing two {@link edu.snu.cay.dolphin.core.optimizer.Cost.ComputeTaskCost} by compute costs.
   */
  private static class CmpCostComparator implements Comparator<Cost.ComputeTaskCost> {
    @Override
    public int compare(final Cost.ComputeTaskCost o1, final Cost.ComputeTaskCost o2) {
      return Double.compare(o1.getComputeCost(), o2.getComputeCost());
    }
  }

  /**
   * Computes a communication cost for the previous execution.
   * The communication cost is calculated by subtracting the maximum compute cost among
   * {@link edu.snu.cay.dolphin.core.ComputeTask}s from the elapsed time between when the
   * controller task started to broadcast and when it finished reduce operation.
   * @param controllerTaskMetrics dolphin metrics collected by {@link edu.snu.cay.dolphin.core.ControllerTask}.
   * @param cmpTaskCosts a collection of costs for {@link edu.snu.cay.dolphin.core.ComputeTask}.
   * @return the calculated communication cost.
   */
  private static double getCommunicationCost(final Map<String, Double> controllerTaskMetrics,
                                             final Collection<Cost.ComputeTaskCost> cmpTaskCosts) {
    // find the maximum compute cost for every compute task.
    final double maxCmpCost = Collections.max(cmpTaskCosts, new CmpCostComparator()).getComputeCost();

    // (communication cost) = (reduce end time) - (broadcast start time) - max(compute cost for each compute task)
    return controllerTaskMetrics.get(DolphinMetricKeys.CONTROLLER_TASK_RECEIVE_DATA_END)
        - controllerTaskMetrics.get(DolphinMetricKeys.CONTROLLER_TASK_SEND_DATA_START)
        - maxCmpCost;
  }

  /**
   * Class having extracted evaluators for a controller task and compute tasks.
   */
  private static class DolphinTaskParameters {
    private final Map<String, Double> controllerTaskMetrics;
    private final Collection<EvaluatorParameters> computeTasksParameters;

    public DolphinTaskParameters(final Map<String, Double> controllerTaskMetrics,
                                 final Collection<EvaluatorParameters> computeTasksParameters) {
      this.controllerTaskMetrics = controllerTaskMetrics;
      this.computeTasksParameters = computeTasksParameters;
    }

    public Map<String, Double> getControllerTaskMetrics() {
      return controllerTaskMetrics;
    }

    public Collection<EvaluatorParameters> getComputeTasksParameters() {
      return computeTasksParameters;
    }
  }
}
