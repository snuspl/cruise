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

import static edu.snu.cay.dolphin.core.optimizer.PlanValidationUtils.checkPlan;
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
    final int numComputeTasks = 1;
    final int availableEvaluators = 4;
    final Collection<EvaluatorParameters> activeEvaluators = generateEvaluatorParameters(
        numComputeTasks, new int[][]{{10000}});

    final Plan plan = ilpSolverOptimizer.optimize(activeEvaluators, availableEvaluators);

    checkPlan(activeEvaluators, plan, availableEvaluators);
    assertTrue("At least one evaluator should be added", plan.getEvaluatorsToAdd().size() > 0);
    assertEquals(0, plan.getEvaluatorsToDelete().size());
  }

  /**
   * Test that evaluators are deleted when available evaluators are reduced to half.
   */
  @Test
  public void testHalfNumComputeTasks() {
    final int numComputeTasks = 4;
    final int availableEvaluators = numComputeTasks / 2 + 1; // +1 for including the controller task
    final Collection<EvaluatorParameters> activeEvaluators = generateEvaluatorParameters(
        numComputeTasks, new int[][]{{2500}, {2500}, {2500}, {2500}});
    final int lowerBoundToDelete = (int) Math.ceil((double) numComputeTasks / 2);

    final Plan plan =
        ilpSolverOptimizer.optimize(activeEvaluators, availableEvaluators);

    checkPlan(activeEvaluators, plan, availableEvaluators);
    assertEquals(0, plan.getEvaluatorsToAdd().size());
    assertTrue("The number of evaluators to be deleted should be >= " + lowerBoundToDelete,
        plan.getEvaluatorsToDelete().size() >= lowerBoundToDelete);
  }

  /**
   * Test that evaluators are added when available evaluators are doubled with multiple types of data.
   */
  @Test
  public void testDoubleComputeTasksWithMultipleDataTypes() {
    final int numComputeTasks = 3;
    final int availableEvaluators = numComputeTasks * 2 + 1;
    final Collection<EvaluatorParameters> activeEvaluators = generateEvaluatorParameters(
        numComputeTasks, new int[][]{{4000, 1000}, {2000, 2000}, {5000, 2000}});

    final Plan plan = ilpSolverOptimizer.optimize(activeEvaluators, availableEvaluators);

    checkPlan(activeEvaluators, plan, availableEvaluators);
    assertTrue("At least one evaluator should be added", plan.getEvaluatorsToAdd().size() > 0);
    assertEquals(0, plan.getEvaluatorsToDelete().size());
  }

  /**
   * Test that evaluators are deleted when available evaluators are reduce to half with multiple types of data.
   */
  @Test
  public void testHalfComputeTasksWithMultipleDataTypes() {
    final int numComputeTasks = 5;
    final int availableEvaluators = numComputeTasks / 2 + 1;
    final Collection<EvaluatorParameters> activeEvaluators = generateEvaluatorParameters(
        numComputeTasks, new int[][]{{2000, 500}, {3000, 1000}, {3000, 1000}, {2500, 2000}, {2500, 2000}});
    final int lowerBoundToDelete = (int) Math.ceil((double) numComputeTasks / 2);

    final Plan plan = ilpSolverOptimizer.optimize(activeEvaluators, availableEvaluators);

    checkPlan(activeEvaluators, plan, availableEvaluators);
    assertEquals(0, plan.getEvaluatorsToAdd().size());
    assertTrue("The number of evaluators to be deleted should be >= " + lowerBoundToDelete,
        plan.getEvaluatorsToDelete().size() >= lowerBoundToDelete);
  }

  /**
   * Test that two compute tasks send data to a new generated compute task.
   */
  @Test
  public void testTwoSenderAndOneReceiver() {
    final int numComputeTasks = 2;
    final int availableEvaluators = 4; // +1 for adding one more compute task and +1 for including the controller task
    final Collection<EvaluatorParameters> activeEvaluators =
        generateEvaluatorParameters(numComputeTasks, new int[][]{{1300}, {900}});

    final Plan plan = ilpSolverOptimizer.optimize(activeEvaluators, availableEvaluators);

    checkPlan(activeEvaluators, plan, availableEvaluators);
    assertEquals(1, plan.getEvaluatorsToAdd().size());
    assertEquals(0, plan.getEvaluatorsToDelete().size());
  }

  /**
   * Generate a collection of evaluator parameters that consists of one controller task
   * and the specified number of compute tasks.
   * @param numComputeTasks the number of compute tasks that participate in the execution.
   * @param dataUnitsArray a two dimension array whose column contains the number of data units for each data type
   *                       and whose row contains data units for each task.
   * @return a collection of evaluator parameters.
   */
  private Collection<EvaluatorParameters> generateEvaluatorParameters(final int numComputeTasks,
                                                                      final int[][] dataUnitsArray) {
    final List<EvaluatorParameters> ret = new ArrayList<>(numComputeTasks + 1);
    double maxComputeTaskEndTime = 0D;

    if (numComputeTasks != dataUnitsArray.length) {
      throw new IllegalArgumentException("# of compute task should be equal to # of rows of data units array");
    }

    // generate compute tasks
    for (int i = 0; i < numComputeTasks; ++i) {
      final List<DataInfo> cmpTaskDataInfos = new ArrayList<>(1);
      int sumDataUnits = 0;

      for (int j = 0; j < dataUnitsArray[i].length; ++j) {
        final int dataUnits = dataUnitsArray[i][j];
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
