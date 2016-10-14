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

import edu.snu.cay.dolphin.bsp.core.ComputeTask;
import edu.snu.cay.dolphin.bsp.core.CtrlTaskContextIdFetcher;
import edu.snu.cay.dolphin.bsp.core.DolphinMetricKeys;
import edu.snu.cay.dolphin.bsp.core.ControllerTask;
import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.optimizer.impl.EvaluatorParametersImpl;
import edu.snu.cay.services.em.plan.api.Plan;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static edu.snu.cay.dolphin.bsp.core.optimizer.OptimizationOrchestrator.NAMESPACE_DOLPHIN_BSP;
import static edu.snu.cay.dolphin.bsp.core.optimizer.PlanValidationUtils.checkPlan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Class for testing {@link ILPSolverOptimizer}'s behavior.
 */
public final class ILPSolverOptimizerTest {
  private final String ctrlTaskId = ControllerTask.TASK_ID_PREFIX + 2;
  private ILPSolverOptimizer ilpSolverOptimizer;
  private Random random;

  @Before
  public void setUp() throws InjectionException {
    final CtrlTaskContextIdFetcher mockFetcher = mock(CtrlTaskContextIdFetcher.class);
    when(mockFetcher.getCtrlTaskContextId()).thenReturn(Optional.of(ctrlTaskId));
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(CtrlTaskContextIdFetcher.class, mockFetcher);
    ilpSolverOptimizer = injector.getInstance(ILPSolverOptimizer.class);
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
    final int numAvailableEvals = 4;
    final Map<String, List<EvaluatorParameters>> activeEvaluators = generateEvaluatorParameters(
        NAMESPACE_DOLPHIN_BSP, numComputeTasks, new int[]{10000});

    final Plan plan = ilpSolverOptimizer.optimize(activeEvaluators, numAvailableEvals, Collections.emptyMap());

    checkPlan(activeEvaluators, plan, numAvailableEvals);
    assertTrue("At least one evaluator should be added", plan.getEvaluatorsToAdd(NAMESPACE_DOLPHIN_BSP).size() > 0);
    assertEquals(0, plan.getEvaluatorsToDelete(NAMESPACE_DOLPHIN_BSP).size());
  }

  /**
   * Test that evaluators are deleted when available evaluators are reduced to half.
   */
  @Test
  public void testHalfNumComputeTasks() {
    final int numComputeTasks = 4;
    final int numAvailableEvals = numComputeTasks / 2 + 1; // +1 for including the controller task
    final Map<String, List<EvaluatorParameters>> activeEvaluators = generateEvaluatorParameters(
        NAMESPACE_DOLPHIN_BSP, numComputeTasks, new int[]{2500, 2500, 2500, 2500});
    final int lowerBoundToDelete = (int) Math.ceil((double) numComputeTasks / 2);

    final Plan plan =
        ilpSolverOptimizer.optimize(activeEvaluators, numAvailableEvals, Collections.emptyMap());

    checkPlan(activeEvaluators, plan, numAvailableEvals);
    assertEquals(0, plan.getEvaluatorsToAdd(NAMESPACE_DOLPHIN_BSP).size());
    assertTrue("The number of evaluators to be deleted should be >= " + lowerBoundToDelete,
        plan.getEvaluatorsToDelete(NAMESPACE_DOLPHIN_BSP).size() >= lowerBoundToDelete);
  }

  /**
   * Test that two compute tasks send data to a new generated compute task.
   */
  @Test
  public void testTwoSenderAndOneReceiver() {
    final int numComputeTasks = 2;
    final int numAvailableEvals = 4; // +1 for adding one more compute task and +1 for including the controller task
    final Map<String, List<EvaluatorParameters>> activeEvaluators =
        generateEvaluatorParameters(NAMESPACE_DOLPHIN_BSP, numComputeTasks, new int[]{1300, 900});

    final Plan plan = ilpSolverOptimizer.optimize(activeEvaluators, numAvailableEvals, Collections.emptyMap());

    checkPlan(activeEvaluators, plan, numAvailableEvals);
    assertEquals(1, plan.getEvaluatorsToAdd(NAMESPACE_DOLPHIN_BSP).size());
    assertEquals(0, plan.getEvaluatorsToDelete(NAMESPACE_DOLPHIN_BSP).size());
  }

  /**
   * Test {@link ILPSolverOptimizer}'s behavior when the controller task cannot be identified.
   * An empty plan should be returned.
   */
  @Test
  public void testWrongCtrlTaskId() throws InjectionException {
    final CtrlTaskContextIdFetcher mockFetcher = mock(CtrlTaskContextIdFetcher.class);
    when(mockFetcher.getCtrlTaskContextId()).thenReturn(Optional.of("##WRONG_CONTROLLER_TASK_ID##"));
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(CtrlTaskContextIdFetcher.class, mockFetcher);

    final ILPSolverOptimizer wrongCtrlIlpSolverOptimizer = injector.getInstance(ILPSolverOptimizer.class);
    final int numComputeTasks = 1;
    final int numAvailableEvals = 4;
    final Map<String, List<EvaluatorParameters>> activeEvaluators = generateEvaluatorParameters(
        NAMESPACE_DOLPHIN_BSP, numComputeTasks, new int[]{10000});

    final Plan plan = wrongCtrlIlpSolverOptimizer.optimize(activeEvaluators, numAvailableEvals, Collections.emptyMap());

    checkPlan(activeEvaluators, plan, numAvailableEvals);
    assertEquals("The plan should be empty", 0, plan.getEvaluatorsToAdd(NAMESPACE_DOLPHIN_BSP).size());
    assertEquals("The plan should be empty", 0, plan.getEvaluatorsToDelete(NAMESPACE_DOLPHIN_BSP).size());
    assertEquals("The plan should be empty", 0, plan.getTransferSteps(NAMESPACE_DOLPHIN_BSP).size());
  }

  /**
   * Generate a collection of evaluator parameters that consists of one controller task
   * and the specified number of compute tasks.
   * @param namespace the namespace of the evaluators to distinguish Evaluators.
   * @param numComputeTasks the number of compute tasks that participate in the execution.
   * @param dataBlocksArray an array that contains the number of data blocks for each task.
   * @return a collection of evaluator parameters.
   */
  private Map<String, List<EvaluatorParameters>> generateEvaluatorParameters(final String namespace,
                                                                             final int numComputeTasks,
                                                                             final int[] dataBlocksArray) {
    final List<EvaluatorParameters> evalParamsList = new ArrayList<>(numComputeTasks + 1);
    double maxComputeTaskEndTime = 0D;

    if (numComputeTasks != dataBlocksArray.length) {
      throw new IllegalArgumentException("# of compute task should be equal to # of rows of data blocks array");
    }

    // generate compute tasks
    for (int i = 0; i < numComputeTasks; ++i) {
      final int numBlocks = dataBlocksArray[i];
      final DataInfo dataInfo = new DataInfoImpl(numBlocks);

      final Map<String, Double> cmpTaskMetrics = new HashMap<>();
      final double computeTaskEndTime = (random.nextDouble() + 1) * numBlocks;
      if (computeTaskEndTime > maxComputeTaskEndTime) {
        maxComputeTaskEndTime = computeTaskEndTime;
      }
      cmpTaskMetrics.put(DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_START, 0D);
      cmpTaskMetrics.put(DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_END, computeTaskEndTime);

      evalParamsList.add(new EvaluatorParametersImpl(ComputeTask.TASK_ID_PREFIX + i, dataInfo, cmpTaskMetrics));
    }

    // generate a controller task
    final Map<String, Double> metrics = new HashMap<>();
    metrics.put(DolphinMetricKeys.CONTROLLER_TASK_SEND_DATA_START, 0D);
    metrics.put(DolphinMetricKeys.CONTROLLER_TASK_RECEIVE_DATA_END, maxComputeTaskEndTime + random.nextInt(50));

    evalParamsList.add(new EvaluatorParametersImpl(ctrlTaskId, new DataInfoImpl(), metrics));

    final Map<String, List<EvaluatorParameters>> evalParamsMap = new HashMap<>(1);
    evalParamsMap.put(namespace, evalParamsList);
    return evalParamsMap;
  }
}
