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
package edu.snu.cay.dolphin.async.optimizer;

import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.dolphin.async.optimizer.parameters.DelayAfterOptimizationMs;
import edu.snu.cay.dolphin.async.plan.PlanImpl;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.impl.LoggingPlanExecutor;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.*;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test the orchestrator correctly processes metrics from MetricHub and provides them to Optimizer.
 */
public final class OptimizationOrchestratorTest {
  private static final String EVAL_PREFIX = "EVAL";
  private static final long OPTIMIZATION_DELAY = 0; // optimizer does no actual reconfiguration

  // configuration variables for calibrating the waiting time for a plan execution
  private static final int MAX_WAIT_LOOP = 20;
  private static final int WAIT_TIME = 10;

  private OptimizationOrchestrator orchestrator;
  private Optimizer optimizer;

  private MetricManager metricManager;

  // Key in these storeId maps will represent the actual active evaluators in the system.
  private Map<Integer, Set<Integer>> workerStoreIdMap;
  private Map<Integer, Set<Integer>> serverStoreIdMap;

  // Default number of blocks for the test.
  private static final int NUM_BLOCKS = 10;

  // Max number of evaluators in each worker and server space.
  private static final int MAX_EVALS = 10;

  /**
   * Setup orchestrator with a fake optimizer and two ElasticMemory instances.
   * ElasticMemory instances are for checking the number of active evaluators running in the system.
   */
  @Before
  public void setUp() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindImplementation(PlanExecutor.class, LoggingPlanExecutor.class)
            .bindNamedParameter(DelayAfterOptimizationMs.class, String.valueOf(OPTIMIZATION_DELAY))
            .build());

    metricManager = injector.getInstance(MetricManager.class);
    final Map<String, Integer> evalIdToNumBlocksMapForWorkers = new HashMap<>();
    final Map<String, Integer> evalIdToNumBlocksMapForServers = new HashMap<>();
    for (int i = 0; i < MAX_EVALS; i++) {
      evalIdToNumBlocksMapForServers.put(EVAL_PREFIX + i, NUM_BLOCKS);
      evalIdToNumBlocksMapForWorkers.put(EVAL_PREFIX + i, NUM_BLOCKS);
    }

    metricManager.loadMetricValidationInfo(evalIdToNumBlocksMapForWorkers, evalIdToNumBlocksMapForServers);
    metricManager.startMetricCollection();

    final ElasticMemory workerEM = mock(ElasticMemory.class);
    workerStoreIdMap = new HashMap<>();

    final ElasticMemory serverEM = mock(ElasticMemory.class);
    serverStoreIdMap = new HashMap<>();

    // return storeIdMaps when orchestrator tries to obtain the actual number of evaluators
    when(workerEM.getStoreIdToBlockIds()).thenReturn(workerStoreIdMap);
    when(serverEM.getStoreIdToBlockIds()).thenReturn(serverStoreIdMap);

    injector.bindVolatileParameter(WorkerEM.class, workerEM);
    injector.bindVolatileParameter(ServerEM.class, serverEM);

    optimizer = mock(Optimizer.class);
    injector.bindVolatileInstance(Optimizer.class, optimizer);

    when(optimizer.optimize(anyMap(), anyInt())).then(new Answer<Plan>() {
      @Override
      public Plan answer(final InvocationOnMock invocation) throws Throwable {
        final Map<String, List<EvaluatorParameters>> evalParamsMap = invocation.getArgumentAt(0, Map.class);

        final List<EvaluatorParameters> serverEvalParams = evalParamsMap.get(Constants.NAMESPACE_SERVER);
        final List<EvaluatorParameters> workerEvalParams = evalParamsMap.get(Constants.NAMESPACE_WORKER);

        assertEquals("Optimizer is triggered with incomplete server metrics",
            serverEvalParams.size(), serverStoreIdMap.size());
        assertEquals("Optimizer is triggered with incomplete worker metrics",
            workerEvalParams.size(), workerStoreIdMap.size());

        final int numUniqueServers = (int) serverEvalParams.stream().map(EvaluatorParameters::getId).distinct().count();
        final int numUniqueWorkers = (int) workerEvalParams.stream().map(EvaluatorParameters::getId).distinct().count();

        assertEquals("Optimizer is triggered with duplicate server metrics", serverEvalParams.size(), numUniqueServers);
        assertEquals("Optimizer is triggered with duplicate worker metrics", workerEvalParams.size(), numUniqueWorkers);

        return PlanImpl.newBuilder().build();
      }
    });

    orchestrator = injector.getInstance(OptimizationOrchestrator.class);
  }

  /**
   * Test that orchestrator triggers optimization, if there's a complete set of metrics.
   */
  @Test
  public void testOptimizerTriggering() {
    final int numServers = 5;
    final int numWorkers = 5;

    for (int i = 0; i < numServers; i++) {
      serverStoreIdMap.put(i, Collections.emptySet());
      final ServerMetrics serverMetrics = ServerMetrics.newBuilder()
          .setNumModelBlocks(NUM_BLOCKS)
          .setTotalReqProcessed(1)
          .build();
      metricManager.storeServerMetrics(EVAL_PREFIX + i, serverMetrics);
    }
    for (int i = 0; i < numWorkers; i++) {
      workerStoreIdMap.put(i, Collections.emptySet());
      final WorkerMetrics workerMetrics = WorkerMetrics.newBuilder()
          .setNumDataBlocks(NUM_BLOCKS)
          .setProcessedDataItemCount(1)
          .setTotalCompTime(1.0)
          .setMiniBatchIdx(0)
          .setTotalTime(0.0)
          .setTotalPullTime(0.0)
          .setTotalPushTime(0.0)
          .setAvgPullTime(0.0)
          .setAvgPushTime(0.0)
          .build();
      metricManager.storeWorkerMetrics(EVAL_PREFIX + i, workerMetrics);
    }

    orchestrator.run();

    waitPlanExecuting();
    verify(optimizer, times(1)).optimize(anyMap(), anyInt());
  }

  /**
   * Test that orchestrator aggregates a complete set of metrics from all active evaluators.
   */
  @Test
  public void testIncompleteMetricAggregating() {
    final int numServers = 5;
    final int numWorkers = 5;

    for (int i = 0; i < numServers; i++) {
      serverStoreIdMap.put(i, Collections.emptySet());
    }
    for (int i = 0; i < numWorkers; i++) {
      workerStoreIdMap.put(i, Collections.emptySet());
    }

    for (int i = 0; i < numServers; i++) {
      final ServerMetrics serverMetrics = ServerMetrics.newBuilder()
          .setNumModelBlocks(NUM_BLOCKS)
          .setTotalReqProcessed(1)
          .build();
      metricManager.storeServerMetrics(EVAL_PREFIX + i, serverMetrics);
      orchestrator.run();

      waitPlanExecuting();
      verify(optimizer, never()).optimize(anyMap(), anyInt());
    }

    for (int i = 0; i < numWorkers; i++) {
      final WorkerMetrics workerMetrics = WorkerMetrics.newBuilder()
          .setNumDataBlocks(NUM_BLOCKS)
          .setProcessedDataItemCount(1)
          .setTotalCompTime(1.0)
          .setMiniBatchIdx(0)
          .setTotalTime(0.0)
          .setTotalPullTime(0.0)
          .setTotalPushTime(0.0)
          .setAvgPullTime(0.0)
          .setAvgPushTime(0.0)
          .build();
      metricManager.storeWorkerMetrics(EVAL_PREFIX + i, workerMetrics);
      orchestrator.run();

      waitPlanExecuting();
      if (i == numWorkers - 1) {
        verify(optimizer, times(1)).optimize(anyMap(), anyInt());
      } else {
        verify(optimizer, never()).optimize(anyMap(), anyInt());
      }
    }
  }

  /**
   * Test that orchestrator filters duplicate metrics.
   */
  @Test
  public void testDuplicateMetricFiltering() {
    final int numServers = 5;
    final int numWorkers = 5;

    for (int i = 0; i < numServers; i++) {
      serverStoreIdMap.put(i, Collections.emptySet());
      final ServerMetrics serverMetrics = ServerMetrics.newBuilder()
          .setNumModelBlocks(NUM_BLOCKS)
          .setTotalReqProcessed(1)
          .build();
      metricManager.storeServerMetrics(EVAL_PREFIX + i, serverMetrics);

      // put duplicate metrics
      metricManager.storeServerMetrics(EVAL_PREFIX + i, serverMetrics);
    }

    for (int i = 0; i < numWorkers; i++) {
      workerStoreIdMap.put(i, Collections.emptySet());
      final WorkerMetrics workerMetrics = WorkerMetrics.newBuilder()
          .setNumDataBlocks(NUM_BLOCKS)
          .setProcessedDataItemCount(1)
          .setTotalCompTime(1.0)
          .setMiniBatchIdx(0)
          .setTotalTime(0.0)
          .setTotalPullTime(0.0)
          .setTotalPushTime(0.0)
          .setAvgPullTime(0.0)
          .setAvgPushTime(0.0)
          .build();
      metricManager.storeWorkerMetrics(EVAL_PREFIX + i, workerMetrics);

      // put duplicate metrics
      metricManager.storeWorkerMetrics(EVAL_PREFIX + i, workerMetrics);
    }

    // check whether it can filter the metrics and finally trigger the optimizer with refined metrics
    orchestrator.run();

    waitPlanExecuting();
    verify(optimizer, times(1)).optimize(anyMap(), anyInt());
  }

  /**
   * Wait until the orchestrator finishes the plan execution.
   */
  private void waitPlanExecuting() {
    int numLoop = 0;
    while (orchestrator.isPlanExecuting() && numLoop < MAX_WAIT_LOOP) {
      ++numLoop;
      try {
        Thread.sleep(WAIT_TIME);
      } catch (final InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting the completion of plan execution");
      }
    }
  }
}
