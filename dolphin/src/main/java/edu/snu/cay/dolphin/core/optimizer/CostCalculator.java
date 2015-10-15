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

  private CostCalculator() {
  }

  /**
   * Returns the evaluator parameters for a controller task.
   * {@link RuntimeException} will be thrown if the evaluator parameters for a controller task does not exist
   * in the specified collection.
   * @param evaluators a collection of {@link EvaluatorParameters}
   * @return the evaluator parameters for a controller task
   */
  private static EvaluatorParameters getControllerTaskParameters(final Collection<EvaluatorParameters> evaluators) {
    for (final EvaluatorParameters evaluator : evaluators) {
      if (evaluator.getId().startsWith(ControllerTask.TASK_ID_PREFIX)) {
        return evaluator;
      }
    }
    throw new RuntimeException("There is no controller task among active evaluators");
  }

  /**
   * @param evaluators a collection of {@link EvaluatorParameters}
   * @return a collection of evaluator parameters for compute tasks
   */
  private static Collection<EvaluatorParameters> getComputeTaskParameters(
      final Collection<EvaluatorParameters> evaluators) {
    final List<EvaluatorParameters> ret = new ArrayList<>();
    for (final EvaluatorParameters evaluator : evaluators) {
      if (evaluator.getId().startsWith(ComputeTask.TASK_ID_PREFIX)) {
        ret.add(evaluator);
      }
    }
    return ret;
  }

  /**
   * Calculates costs of Dolphin.
   * @return the calculated costs
   */
  public static Cost calculate(final Collection<EvaluatorParameters> activeEvaluators) {
    final EvaluatorParameters ctlTaskEvaluatorParameters = getControllerTaskParameters(activeEvaluators);
    final Collection<EvaluatorParameters> cmpTasksEvaluatorParameters = getComputeTaskParameters(activeEvaluators);
    final Collection<Cost.ComputeTaskCost> computeTaskCosts = getComputeTaskCosts(cmpTasksEvaluatorParameters);
    final double communicationCost = getCommunicationCost(ctlTaskEvaluatorParameters, computeTaskCosts);
    return new Cost(communicationCost, computeTaskCosts);
  }

  /**
   * @param computeTasksParameters a collection of evaluator parameters for compute tasks
   * @return a collection of costs for {@link ComputeTask}s
   */
  private static Collection<Cost.ComputeTaskCost> getComputeTaskCosts(
      final Collection<EvaluatorParameters> computeTasksParameters) {
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
   * The communication cost is calculated by subtracting the maximum compute cost among {@link ComputeTask}s
   * from the elapsed time between when the controller task started to broadcast and when it finished reduce operation.
   * @param cmpTaskCosts a collection of costs for {@link ComputeTask}.
   * @return the calculated communication cost.
   */
  private static double getCommunicationCost(final EvaluatorParameters controllerTaskParameters,
                                      final Collection<Cost.ComputeTaskCost> cmpTaskCosts) {
    // find the maximum compute cost for every compute task.
    final double maxCmpCost = Collections.max(cmpTaskCosts, new CmpCostComparator()).getComputeCost();

    // (communication cost) = (reduce end time) - (broadcast start time) - max(compute cost for each compute task)
    return controllerTaskParameters.getMetrics().get(DolphinMetricKeys.CONTROLLER_TASK_RECEIVE_DATA_END)
        - controllerTaskParameters.getMetrics().get(DolphinMetricKeys.CONTROLLER_TASK_SEND_DATA_START)
        - maxCmpCost;
  }
}
