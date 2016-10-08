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

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;
import edu.snu.cay.services.ps.server.parameters.ServerNumThreads;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@link AsyncDolphinOptimizer}'s plan generation according to the cost model described in the class's javadoc.
 *
 * Sample computation time and communication time with data/model block distributions can be assigned to
 * workers and servers respectively to test that the optimizer generates a valid plan.
 */

public final class AsyncDolphinOptimizerTest {
  private static final String WORKER_PREFIX = "Worker-";
  private static final String SERVER_PREFIX = "Server-";

  private AsyncDolphinOptimizer optimizer;

  private static List<EvaluatorParameters> workerEvaluatorParameters;
  private static List<EvaluatorParameters> serverEvaluatorParameters;
  private static Map<String, Double> optimizerModelParamsMap;

  private static int numRunningWorkers;
  private static int numRunningServers;

  /**
   * Sets up metrics that works accordingly with the current cost model.
   * If the metric values are modified, the {@code assertEquals} conditions in the tests below MUST be re-checked.
   * @throws InjectionException
   */
  @Before
  public void setUp() throws InjectionException {
    //The set of pre-calculated values this test will run with
    final int numTotalDataInstances = 285; // the sum of all entries in dataInstancesForWorkers
    final int numTotalModelKeys = 50; // derived using parameter worker metrics in real situations
    final int numServerThreads = 2;
    final int miniBatchSize = 100;
    final int emBlockSize = 10;

    final Injector injector = Tang.Factory.getTang().newInjector(
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindNamedParameter(ServerNumThreads.class, String.valueOf(numServerThreads))
            .bindNamedParameter(Parameters.MiniBatchSize.class, String.valueOf(miniBatchSize))
        .build());

    // worker metrics
    final int[] dataInstancesForWorkers = new int[] {100, 90, 95};
    final double[] compTimeForWorkers = new double[] {100.0, 90.0, 95.0};
    numRunningWorkers = dataInstancesForWorkers.length;
    workerEvaluatorParameters =
        generateWorkerEvaluatorParameters(emBlockSize, dataInstancesForWorkers, compTimeForWorkers);

    // server metrics
    final int[] pullCountForServers = new int[] {50, 60, 50, 50};
    final double[] pullTimeForServers = new double[] {10.0, 15.0, 8.0, 15.0};
    numRunningServers = pullCountForServers.length;
    serverEvaluatorParameters = generateServerEvaluatorParameters(emBlockSize, pullCountForServers, pullTimeForServers);

    optimizerModelParamsMap = new HashMap<>(2);
    optimizerModelParamsMap.put(Constants.TOTAL_DATA_INSTANCES, (double) numTotalDataInstances);
    optimizerModelParamsMap.put(Constants.TOTAL_MODEL_KEYS, (double) numTotalModelKeys);

    optimizer = injector.getInstance(AsyncDolphinOptimizer.class);
  }

  /**
   * Tests for a correct optimization plan on the same number of resources with the current metrics.
   */
  @Test
  public void testIdenticalAvailableResourcesOptimization() {
    final Map<String, List<EvaluatorParameters>> map = new HashMap<>(2, 1);
    map.put(Constants.NAMESPACE_WORKER, workerEvaluatorParameters);
    map.put(Constants.NAMESPACE_SERVER, serverEvaluatorParameters);
    final Plan plan = optimizer.optimize(map, numRunningWorkers + numRunningServers, optimizerModelParamsMap);

    assertEquals(plan.getEvaluatorsToAdd(Constants.NAMESPACE_WORKER).size(), 2);
    assertEquals(plan.getEvaluatorsToAdd(Constants.NAMESPACE_SERVER).size(), 0);
    assertEquals(plan.getEvaluatorsToDelete(Constants.NAMESPACE_WORKER).size(), 0);
    assertEquals(plan.getEvaluatorsToDelete(Constants.NAMESPACE_SERVER).size(), 2);
  }

  /**
   * Tests for a correct optimization plan when a resource is removed with the current metrics.
   */
  @Test
  public void testFewerAvailableResourcesOptimization() {
    final Map<String, List<EvaluatorParameters>> map = new HashMap<>(2, 1);
    map.put(Constants.NAMESPACE_WORKER, workerEvaluatorParameters);
    map.put(Constants.NAMESPACE_SERVER, serverEvaluatorParameters);
    final Plan plan = optimizer.optimize(map, numRunningWorkers + numRunningServers - 1, optimizerModelParamsMap);

    assertEquals(plan.getEvaluatorsToAdd(Constants.NAMESPACE_WORKER).size(), 2);
    assertEquals(plan.getEvaluatorsToAdd(Constants.NAMESPACE_SERVER).size(), 0);
    assertEquals(plan.getEvaluatorsToDelete(Constants.NAMESPACE_WORKER).size(), 0);
    assertEquals(plan.getEvaluatorsToDelete(Constants.NAMESPACE_SERVER).size(), 3);
  }

  /**
   * Tests for a correct optimization plan when an extra resource is added with the current metrics.
   */
  @Test
  public void testMoreAvailableResourcesOptimization() {
    final Map<String, List<EvaluatorParameters>> map = new HashMap<>(2, 1);
    map.put(Constants.NAMESPACE_WORKER, workerEvaluatorParameters);
    map.put(Constants.NAMESPACE_SERVER, serverEvaluatorParameters);
    final Plan plan = optimizer.optimize(map, numRunningWorkers + numRunningServers + 1, optimizerModelParamsMap);

    assertEquals(plan.getEvaluatorsToAdd(Constants.NAMESPACE_WORKER).size(), 3);
    assertEquals(plan.getEvaluatorsToAdd(Constants.NAMESPACE_SERVER).size(), 0);
    assertEquals(plan.getEvaluatorsToDelete(Constants.NAMESPACE_WORKER).size(), 0);
    assertEquals(plan.getEvaluatorsToDelete(Constants.NAMESPACE_SERVER).size(), 2);
  }

  private List<EvaluatorParameters> generateWorkerEvaluatorParameters(final int blockSize,
                                                                      final int[] dataInstancesForWorkers,
                                                                      final double[] compTimeArray) {
    final List<EvaluatorParameters> evalParamList = new ArrayList<>();

    for (int index = 0; index < numRunningWorkers; ++index) {
      final DataInfo dataInfo = new DataInfoImpl((int) Math.ceil(dataInstancesForWorkers[index] / blockSize));
      final WorkerMetrics workerMetrics = WorkerMetrics.newBuilder()
          .setTotalCompTime(compTimeArray[index])
          .setProcessedDataItemCount(dataInstancesForWorkers[index])
          // This metric is used to compare the ground truth with AsyncDolphinOptimizer's calculation results.
          // It is not critical for test purposes; it just needs to be set to run AsyncDolphinOptimizer's code.
          .setTotalPullTime(5.0)
          .build();

      evalParamList.add(new WorkerEvaluatorParameters(WORKER_PREFIX + index, dataInfo, workerMetrics));
    }
    return evalParamList;
  }

  private List<EvaluatorParameters> generateServerEvaluatorParameters(final int blockSize,
                                                                      final int[] pullCountForServers,
                                                                      final double[] pullTimeForServers) {
    final List<EvaluatorParameters> evalParamList = new ArrayList<>(numRunningServers);

    for (int index = 0; index < numRunningServers; ++index) {
      // Although not accurate, it is not critical for test purposes.
      final DataInfo dataInfo = new DataInfoImpl((int) Math.ceil(pullCountForServers[index] / blockSize));
      final ServerMetrics serverMetrics = ServerMetrics.newBuilder()
          .setTotalPullProcessed(pullCountForServers[index])
          .setTotalPullProcessingTimeSec(pullTimeForServers[index]).build();

      evalParamList.add(new ServerEvaluatorParameters(SERVER_PREFIX + index, dataInfo, serverMetrics));
    }
    return evalParamList;
  }
}
