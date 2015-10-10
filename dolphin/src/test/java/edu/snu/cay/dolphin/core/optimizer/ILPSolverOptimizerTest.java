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

/**
 * Class for testing ILPSolverOptimizer.
 */
public final class ILPSolverOptimizerTest {

  private ILPSolverOptimizer ilpSolverOptimizer;
  private Random random;

  /**
   * Setup ILPSolverOptimizer.
   */
  @Before
  public void setUp() throws InjectionException {
    ilpSolverOptimizer = Tang.Factory.getTang().newInjector().getInstance(ILPSolverOptimizer.class);
    random = new Random(System.currentTimeMillis());
  }

  @Test
  public void testOptimize() {
    final Collection<EvaluatorParameters> activeEvaluators = generateEvaluatorParameters(10000);
    final Plan plan = ilpSolverOptimizer.optimize(activeEvaluators, 4);
    System.out.println(plan);
  }

  /**
   * Generate a collection of evaluator parameters that consists of one compute task and one controller task.
   * Assumes only one type of data.
   * @param totalDataUnits the total number of data units.
   * @return a collection of evaluator parameters.
   */
  private Collection<EvaluatorParameters> generateEvaluatorParameters(final int totalDataUnits) {
    final List<EvaluatorParameters> ret = new ArrayList<>(2);

    // compute task
    final List<DataInfo> cmpTaskDataInfos = new ArrayList<>(1);
    cmpTaskDataInfos.add(new DataInfoImpl("testType", totalDataUnits));

    final Map<String, Double> cmpTaskMetrics = new HashMap<>();
    final double computeTaskEndTime = (random.nextDouble() + 1) * totalDataUnits;
    System.out.println("computeTaskEndTime=" + computeTaskEndTime);
    cmpTaskMetrics.put(DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_START, 0D);
    cmpTaskMetrics.put(DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_END, computeTaskEndTime);

    ret.add(new EvaluatorParametersImpl(ComputeTask.TASK_ID_PREFIX + 1, cmpTaskDataInfos, cmpTaskMetrics));

    // controller task
    final Map<String, Double> metrics = new HashMap<>();
    metrics.put(DolphinMetricKeys.CONTROLLER_TASK_SEND_DATA_START, 0D);
    metrics.put(DolphinMetricKeys.CONTROLLER_TASK_RECEIVE_DATA_END, computeTaskEndTime + random.nextInt(50));

    ret.add(new EvaluatorParametersImpl(ControllerTask.TASK_ID_PREFIX + 2, new ArrayList<DataInfo>(0), metrics));

    return ret;
  }
}
