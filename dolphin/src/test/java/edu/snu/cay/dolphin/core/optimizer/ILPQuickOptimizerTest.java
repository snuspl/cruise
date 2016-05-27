/*
 * Copyright (C) 2016 Seoul National University
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
import edu.snu.cay.dolphin.core.CtrlTaskContextIdFetcher;
import edu.snu.cay.dolphin.core.DolphinMetricKeys;
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

import static edu.snu.cay.dolphin.core.optimizer.OptimizationOrchestrator.NAMESPACE_DOLPHIN_BSP;
import static edu.snu.cay.dolphin.core.optimizer.PlanValidationUtils.checkPlan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Class for testing {@link ILPQuickOptimizer}'s behavior.
 * For namespace, {@link OptimizationOrchestrator#NAMESPACE_DOLPHIN_BSP} is used.
 */
public final class ILPQuickOptimizerTest {
  private final String ctrlTaskId = ControllerTask.TASK_ID_PREFIX;
  private ILPQuickOptimizer ilpQuickOptimizer;

  @Before
  public void setUp() throws InjectionException {
    final CtrlTaskContextIdFetcher mockFetcher = mock(CtrlTaskContextIdFetcher.class);
    when(mockFetcher.getCtrlTaskContextId()).thenReturn(Optional.of(ctrlTaskId));
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(CtrlTaskContextIdFetcher.class, mockFetcher);
    ilpQuickOptimizer = injector.getInstance(ILPQuickOptimizer.class);
  }

  /**
   * Test that the optimizer gives a plan to increase the number of participating evaluators,
   * when doing so is clearly beneficial.
   * We create such a situation by setting the communication cost to be very low.
   */
  @Test
  public void testLowCommCost() {
    final int availableEvaluators = 6 + 1; // 1 for the ctrl task
    final Map<String, List<EvaluatorParameters>> activeEvaluators = generateEvaluatorParameters(
        NAMESPACE_DOLPHIN_BSP, new int[][]{{100, 100, 100}, {100, 100, 100}}, 5D);

    final Plan plan = ilpQuickOptimizer.optimize(activeEvaluators, availableEvaluators);

    checkPlan(activeEvaluators, plan, availableEvaluators);
    assertTrue("At least one evaluator should be added", plan.getEvaluatorsToAdd(NAMESPACE_DOLPHIN_BSP).size() > 0);
    assertEquals(0, plan.getEvaluatorsToDelete(NAMESPACE_DOLPHIN_BSP).size());
  }

  /**
   * Test that the optimizer gives a plan to decrease the number of participating evaluators,
   * when doing so is clearly beneficial.
   * We create such a situation by setting the communication cost to be very high.
   */
  @Test
  public void testHighCommCost() {
    final int availableEvaluators = 6 + 1; // 1 for the ctrl task
    final Map<String, List<EvaluatorParameters>> activeEvaluators = generateEvaluatorParameters(
        NAMESPACE_DOLPHIN_BSP, new int[][]{{100, 100, 100}, {100, 100, 100}, {100, 100, 100}, {100, 100, 100}}, 5000D);

    final Plan plan = ilpQuickOptimizer.optimize(activeEvaluators, availableEvaluators);

    checkPlan(activeEvaluators, plan, availableEvaluators);
    assertTrue("At least one evaluator should be deleted",
        plan.getEvaluatorsToDelete(NAMESPACE_DOLPHIN_BSP).size() > 0);
    assertEquals(0, plan.getEvaluatorsToAdd(NAMESPACE_DOLPHIN_BSP).size());
  }

  /**
   * Test that the optimizer gives a plan to decrease the number of participating evaluators,
   * when the number of available evaluators is reduced than before.
   */
  @Test
  public void testAvailableEvalsReduced() {
    final int availableEvaluators = 2 + 1; // 1 for the ctrl task
    final Map<String, List<EvaluatorParameters>> activeEvaluators = generateEvaluatorParameters(
        NAMESPACE_DOLPHIN_BSP, new int[][]{{100, 100, 100}, {100, 100, 100}, {100, 100, 100}, {100, 100, 100}}, 100D);

    final Plan plan = ilpQuickOptimizer.optimize(activeEvaluators, availableEvaluators);

    checkPlan(activeEvaluators, plan, availableEvaluators);
    assertTrue("At least two evaluators should be deleted",
        plan.getEvaluatorsToDelete(NAMESPACE_DOLPHIN_BSP).size() >= 2);
    assertEquals(0, plan.getEvaluatorsToAdd(NAMESPACE_DOLPHIN_BSP).size());
  }

  /**
   * Test {@link ILPQuickOptimizer}'s behavior when the controller task cannot be identified.
   * An empty plan should be returned.
   */
  @Test
  public void testWrongCtrlTaskId() throws InjectionException {
    final CtrlTaskContextIdFetcher mockFetcher = mock(CtrlTaskContextIdFetcher.class);
    when(mockFetcher.getCtrlTaskContextId()).thenReturn(Optional.of("##WRONG_CONTROLLER_TASK_ID##"));
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(CtrlTaskContextIdFetcher.class, mockFetcher);

    final ILPQuickOptimizer wrongCtrlIlpQuickerOptimizer = injector.getInstance(ILPQuickOptimizer.class);
    final int availableEvaluators = 4;
    final Map<String, List<EvaluatorParameters>> activeEvaluators = generateEvaluatorParameters(
        NAMESPACE_DOLPHIN_BSP, new int[][]{{100, 50}, {100, 100}}, 50D);

    final Plan plan = wrongCtrlIlpQuickerOptimizer.optimize(activeEvaluators, availableEvaluators);

    checkPlan(activeEvaluators, plan, availableEvaluators);
    assertEquals("The plan should be empty", 0, plan.getEvaluatorsToAdd(NAMESPACE_DOLPHIN_BSP).size());
    assertEquals("The plan should be empty", 0, plan.getEvaluatorsToDelete(NAMESPACE_DOLPHIN_BSP).size());
    assertEquals("The plan should be empty", 0, plan.getTransferSteps(NAMESPACE_DOLPHIN_BSP).size());
  }

  /**
   * Generate a map of {@link EvaluatorParameters}'s using the given {@code dataArray} and {@code commCost}.
   *
   * The parameter {@code dataArray} is assumed to have {@code n} inner arrays, where {@code n} equals the number of
   * compute tasks.
   * Each inner array of {@code dataArray} should have {@code k + 1} values, where the first {@code k} values represent
   * the number of units for {@code k} distinct types and the last value equals the computation time for
   * that compute task.
   * The parameter {@code commCost} is simply the total communication cost.
   */
  private Map<String, List<EvaluatorParameters>> generateEvaluatorParameters(final String namespace,
                                                                             final int[][] dataArray,
                                                                             final double commCost) {
    final List<EvaluatorParameters> evalParamList = new ArrayList<>(dataArray.length + 1);
    double maxCompCost = 0D;

    for (int index = 0; index < dataArray.length; ++index) {
      final int[] dataForOneCompTask = dataArray[index];
      final List<DataInfo> dataInfoList = new ArrayList<>(dataForOneCompTask.length - 1);
      for (int dataType = 0; dataType < dataForOneCompTask.length - 1; ++dataType) {
        dataInfoList.add(new DataInfoImpl(String.format("testType-%d", dataType), dataForOneCompTask[dataType]));
      }

      final double compCost = (double)dataForOneCompTask[dataForOneCompTask.length - 1];
      final Map<String, Double> cmpTaskMetrics = new HashMap<>();
      cmpTaskMetrics.put(DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_START, 0D);
      cmpTaskMetrics.put(DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_END, compCost);
      evalParamList.add(new EvaluatorParametersImpl(ComputeTask.TASK_ID_PREFIX + index, dataInfoList, cmpTaskMetrics));

      maxCompCost = maxCompCost < compCost ? compCost : maxCompCost;
    }

    final Map<String, Double> ctrlTaskMetrics = new HashMap<>();
    ctrlTaskMetrics.put(DolphinMetricKeys.CONTROLLER_TASK_SEND_DATA_START, 0D);
    ctrlTaskMetrics.put(DolphinMetricKeys.CONTROLLER_TASK_RECEIVE_DATA_END, commCost + maxCompCost);
    evalParamList.add(new EvaluatorParametersImpl(ctrlTaskId, new ArrayList<DataInfo>(0), ctrlTaskMetrics));

    final Map<String, List<EvaluatorParameters>> evalParamsMap = new HashMap<>(1);
    evalParamsMap.put(namespace, evalParamList);
    return evalParamsMap;
  }
}
