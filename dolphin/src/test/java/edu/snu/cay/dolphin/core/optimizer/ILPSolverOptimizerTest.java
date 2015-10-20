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
import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.optimizer.impl.EvaluatorParametersImpl;
import edu.snu.cay.services.em.plan.api.Plan;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Class for testing {@link ILPSolverOptimizer}'s behavior.
 */
public final class ILPSolverOptimizerTest {

  private ILPSolverOptimizer ilpSolverOptimizer;
  private Random random;

  @Before
  public void setUp() throws InjectionException {
    ilpSolverOptimizer = Tang.Factory.getTang().newInjector().getInstance(ILPSolverOptimizer.class);
    random = new Random();
  }

  /**
   * Test that evaluators are added when available evaluators are increased.
   * The number of compute tasks participating the previous execution is set to one so that evaluators should be added
   * otherwise the communication cost of the previous execution is significantly high.
   */
  @Test
  public void testIncreasedComputeTasks() {
    final Collection<EvaluatorParameters> activeEvaluators = generateEvaluatorParameters(1, new int[]{10000});

    final Plan plan = ilpSolverOptimizer.optimize(activeEvaluators, 4);

    assertTrue("At least one evaluator should be added", plan.getEvaluatorsToAdd().size() > 0);
    assertEquals(0, plan.getEvaluatorsToDelete().size());
  }

  /**
   * Test that evaluators are deleted when available evaluators are reduced to half.
   */
  @Test
  public void testHalfNumComputeTasks() {
    final int numComputeTask = 4;
    final Collection<EvaluatorParameters> activeEvaluators =
        generateEvaluatorParameters(numComputeTask, new int[]{10000});
    final int upperBoundToDelete = (int) Math.ceil((double) numComputeTask / 2);

    final Plan plan =
        ilpSolverOptimizer.optimize(activeEvaluators, numComputeTask / 2 + 1); // +1 for including the controller task

    assertEquals(0, plan.getEvaluatorsToAdd().size());
    assertTrue("The number of evaluators to be deleted should be <= " + upperBoundToDelete,
        plan.getEvaluatorsToDelete().size() <= upperBoundToDelete);
  }

  /**
   * Generate a collection of evaluator parameters that consists of one controller task
   * and the specified number of compute tasks.
   * For each data type, data units will be evenly distributed to compute tasks.
   * @param numComputeTasks the number of compute tasks that participate in the execution.
   * @param dataUnitsArray an array of the number of data units for each data type.
   * @return a collection of evaluator parameters.
   */
  private Collection<EvaluatorParameters> generateEvaluatorParameters(final int numComputeTasks,
                                                                      final int[] dataUnitsArray) {
    final List<EvaluatorParameters> ret = new ArrayList<>(numComputeTasks + 1);
    double maxComputeTaskEndTime = 0D;

    // generate compute tasks
    for (int i = 0; i < numComputeTasks; ++i) {
      final List<DataInfo> cmpTaskDataInfos = new ArrayList<>(1);
      int sumDataUnits = 0;

      for (int j = 0; j < dataUnitsArray.length; ++j) {
        final int dataUnitsPerEval = dataUnitsArray[j] / numComputeTasks;
        final int dataUnits = (i == 0) ? dataUnitsPerEval + (dataUnitsArray[j] % numComputeTasks) : dataUnitsPerEval;

        cmpTaskDataInfos.add(new DataInfoImpl(String.format("testType-%d", j), dataUnits));
        sumDataUnits += dataUnits;
      }

      final Map<String, Double> cmpTaskMetrics = new HashMap<>();
      final double computeTaskEndTime = (random.nextDouble() + 1) * sumDataUnits;
      if (computeTaskEndTime > maxComputeTaskEndTime) {
        maxComputeTaskEndTime = computeTaskEndTime;
      }
      cmpTaskMetrics.put(DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_START, 0D);
      cmpTaskMetrics.put(DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_END, computeTaskEndTime);

      ret.add(new EvaluatorParametersImpl(ComputeTask.TASK_ID_PREFIX + i, cmpTaskDataInfos, cmpTaskMetrics));
    }

    // generate a controller task
    final Map<String, Double> metrics = new HashMap<>();
    metrics.put(DolphinMetricKeys.CONTROLLER_TASK_SEND_DATA_START, 0D);
    metrics.put(DolphinMetricKeys.CONTROLLER_TASK_RECEIVE_DATA_END, maxComputeTaskEndTime + random.nextInt(50));

    ret.add(new EvaluatorParametersImpl(ControllerTask.TASK_ID_PREFIX + 2, new ArrayList<DataInfo>(0), metrics));

    return ret;
  }
}
