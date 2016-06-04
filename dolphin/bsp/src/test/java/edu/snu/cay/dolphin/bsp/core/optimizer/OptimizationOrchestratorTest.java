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

import edu.snu.cay.dolphin.bsp.core.StageInfo;
import edu.snu.cay.dolphin.bsp.core.UserJobInfo;
import edu.snu.cay.dolphin.bsp.examples.simple.SimpleCmpTask;
import edu.snu.cay.dolphin.bsp.examples.simple.SimpleCommGroup;
import edu.snu.cay.dolphin.bsp.examples.simple.SimpleCtrlTask;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.optimizer.impl.RandomOptimizer;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import edu.snu.cay.services.em.plan.impl.LoggingPlanExecutor;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test the orchestrator.
 */
public final class OptimizationOrchestratorTest {
  private OptimizationOrchestrator orchestrator;

  /**
   * Setup orchestration with a RandomOptimizer that calls a mock ElasticMemory instance.
   */
  @Before
  public void setUp() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(
        getRandomOptimizerConfiguration());
    injector.bindVolatileInstance(ElasticMemory.class, mock(ElasticMemory.class));

    final List<StageInfo> stageInfoList = new ArrayList<>(1);
    stageInfoList.add(
        StageInfo.newBuilder(SimpleCmpTask.class, SimpleCtrlTask.class, SimpleCommGroup.class)
            .setOptimizable(true)
            .build());
    final UserJobInfo mockUserJobInfo = mock(UserJobInfo.class);
    when(mockUserJobInfo.getStageInfoList()).thenReturn(stageInfoList);
    injector.bindVolatileInstance(UserJobInfo.class, mockUserJobInfo);

    orchestrator = injector.getInstance(OptimizationOrchestrator.class);
  }

  /**
   * @return a configuration with RandomOptimizer and LoggingPlanExecutor
   */
  private static Configuration getRandomOptimizerConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(Optimizer.class, RandomOptimizer.class)
        .bindImplementation(PlanExecutor.class, LoggingPlanExecutor.class)
        .build();
  }

  /**
   * Tests that each plan execution starts only after all metrics messages are received,
   * across multiple iterations.
   */
  @Test
  public void testMultipleIterations() throws ExecutionException, InterruptedException, TimeoutException {
    // Start five running tasks (1 controller + 4 compute)
    for (int i = 0; i < 5; i++) {
      orchestrator.onRunningTask(mock(RunningTask.class));
    }

    Future<PlanResult> previousResult = null;
    for (int i = 0; i < 5; i++) {
      previousResult = run(i, previousResult);
    }
  }

  private Future<PlanResult> run(final int iteration, final Future<PlanResult> previousResult)
      throws ExecutionException, InterruptedException, TimeoutException {
    final String groupName = SimpleCommGroup.class.getName();

    assertEquals(previousResult, orchestrator.getPlanExecutionResult());
    orchestrator.receiveComputeMetrics("context-0", groupName, iteration, getComputeTaskMetrics(),
        new DataInfoImpl(1000));
    assertEquals(previousResult, orchestrator.getPlanExecutionResult());
    orchestrator.receiveComputeMetrics("context-1", groupName, iteration, getComputeTaskMetrics(),
        new DataInfoImpl(2000));
    assertEquals(previousResult, orchestrator.getPlanExecutionResult());
    orchestrator.receiveControllerMetrics("context-controller", groupName, iteration, getControllerTaskMetrics());
    assertEquals(previousResult, orchestrator.getPlanExecutionResult());
    orchestrator.receiveComputeMetrics("context-2", groupName, iteration, getComputeTaskMetrics(),
        new DataInfoImpl(3000));
    assertEquals(previousResult, orchestrator.getPlanExecutionResult());
    orchestrator.receiveComputeMetrics("context-3", groupName, iteration, getComputeTaskMetrics(),
        new DataInfoImpl(4000));

    // All metrics received; optimization should have run
    orchestrator.getOptimizationAttemptResult().get(1, TimeUnit.SECONDS);
    orchestrator.getPlanExecutionResult().get(1, TimeUnit.SECONDS);
    assertNotEquals(previousResult, orchestrator.getPlanExecutionResult());
    return orchestrator.getPlanExecutionResult();
  }

  private Map<String, Double> getComputeTaskMetrics() {
    final Map<String, Double> metrics = new HashMap<>();
    return metrics;
  }

  private Map<String, Double> getControllerTaskMetrics() {
    final Map<String, Double> metrics = new HashMap<>();
    return metrics;
  }
}
