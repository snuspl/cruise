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

import edu.snu.cay.dolphin.async.metric.WorkerConstants;
import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.optimizer.impl.EvaluatorParametersImpl;
import edu.snu.cay.services.ps.metric.ServerConstants;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for testing {@link AsyncDolphinOptimizer}'s behavior.
 */
public final class AsyncDolphinOptimizerTest {
  private static final String WORKER_PREFIX = "Worker-";
  private static final String SERVER_PREFIX = "Server-";
  private AsyncDolphinOptimizer optimizer;

  @Before
  public void setUp() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    optimizer = injector.getInstance(AsyncDolphinOptimizer.class);
  }

  @Test
  public void testAnything() {
    final int[] dataArray = new int[]{30, 20, 40, 10, 50};
    final double[] elapsedTimeArray = new double[]{30, 60, 24, 8, 60};
    final List<EvaluatorParameters> workerEvaluatorParameters =
        generateWorkerEvaluatorParameters(dataArray, elapsedTimeArray);

    final int[] numModelsArray = new int[]{400, 500, 300, 100, 200};
    final double[] processingTimeArray = new double[]{0.006, 0.010, 0.004, 0.008, 0.030};
    final List<EvaluatorParameters> serverEvaluatorParameters =
        generateServerEvaluatorParameters(numModelsArray, processingTimeArray);

    final Map<String, List<EvaluatorParameters>> map = new HashMap<>(2, 1);
    map.put(OptimizationOrchestrator.NAMESPACE_SERVER, serverEvaluatorParameters);
    map.put(OptimizationOrchestrator.NAMESPACE_WORKER, workerEvaluatorParameters);
    optimizer.optimize(map, 10);
  }

  private List<EvaluatorParameters> generateServerEvaluatorParameters(final int[] numModelsArray,
                                                                      final double[] processingTimeArray) {
    final List<EvaluatorParameters> evalParamList = new ArrayList<>(numModelsArray.length);

    for (int index = 0; index < numModelsArray.length; ++index) {
      final DataInfo dataInfo = new DataInfoImpl(numModelsArray[index]);
      final Map<String, Double> serverMetrics = new HashMap<>();
      serverMetrics.put(ServerConstants.KEY_SERVER_PROCESSING_UNIT, processingTimeArray[index]);

      evalParamList.add(new EvaluatorParametersImpl(SERVER_PREFIX + index, dataInfo, serverMetrics));
    }

    return evalParamList;
  }

  private List<EvaluatorParameters> generateWorkerEvaluatorParameters(final int[] dataArray,
                                                                      final double[] elapsedTimeArray) {
    final List<EvaluatorParameters> evalParamList = new ArrayList<>(dataArray.length);

    for (int index = 0; index < dataArray.length; ++index) {
      final DataInfo dataInfo = new DataInfoImpl(dataArray[index]);
      final Map<String, Double> workerMetrics = new HashMap<>();
      workerMetrics.put(WorkerConstants.KEY_WORKER_COMPUTE_TIME, elapsedTimeArray[index]);

      evalParamList.add(new EvaluatorParametersImpl(WORKER_PREFIX + index, dataInfo, workerMetrics));
    }

    return evalParamList;
  }
}
