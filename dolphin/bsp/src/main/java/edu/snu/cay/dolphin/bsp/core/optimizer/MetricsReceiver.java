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

import edu.snu.cay.services.em.optimizer.api.DataInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Keeps track of metrics messages, and runs optimization when all metrics messages are received.
 * For use within the OptimizationOrchestrator.
 */
final class MetricsReceiver {
  private static final Logger LOG = Logger.getLogger(MetricsReceiver.class.getName());

  private final OptimizationOrchestrator optimizationOrchestrator;

  /**
   * The DataInfo for each compute task (keyed by the compute task's contextId).
   */
  private final Map<String, DataInfo> computeIdToDataInfo;

  /**
   * A Map of metrics (metricId, metricValue) for each compute task (keyed by the compute task's contextId).
   */
  private final Map<String, Map<String, Double>> computeIdToMetrics;

  /**
   * The controller task's contextId.
   */
  private String controllerId;

  /**
   * The Map of metrics (metricId, metricValue) for controller task.
   */
  private Map<String, Double> controllerMetrics;

  /**
   * Whether this iteration can be optimized.
   */
  private boolean optimizable;

  /**
   * The number of tasks for which metrics should be received.
   * This value is set at construction time, at the beginning of the iteration.
   */
  private final int numTasks;

  /**
   * The number of task metrics that have been received.
   * Optimization is started when this value reaches numTasks.
   */
  private int numTasksReceived = 0;

  /**
   * @param optimizationOrchestrator the optimization orchestrator that instantiates this instance
   */
  public MetricsReceiver(final OptimizationOrchestrator optimizationOrchestrator,
                         final boolean optimizable,
                         final int numTasks) {
    this.optimizationOrchestrator = optimizationOrchestrator;
    this.optimizable = optimizable;
    this.numTasks = numTasks;
    this.computeIdToMetrics = new HashMap<>();
    this.computeIdToDataInfo = new HashMap<>();
  }

  public synchronized void addCompute(final String contextId,
                                      final Map<String, Double> metrics,
                                      final DataInfo dataInfo) {
    computeIdToMetrics.put(contextId, metrics);
    computeIdToDataInfo.put(contextId, dataInfo);
    numTasksReceived++;
    LOG.log(Level.FINE, "numTasksReceived {0}", numTasksReceived);

    checkAndRunOptimization();
  }

  public synchronized void addController(final String contextId,
                                         final Map<String, Double> metrics) {
    this.controllerId = contextId;
    this.controllerMetrics = metrics;
    numTasksReceived++;
    LOG.log(Level.FINE, "numTasksReceived {0}", numTasksReceived);

    checkAndRunOptimization();
  }

  private void checkAndRunOptimization() {
    if (numTasksReceived == numTasks) {
      if (optimizable) {
        optimizationOrchestrator.run(computeIdToDataInfo, computeIdToMetrics, controllerId, controllerMetrics);
      } else {
        LOG.log(Level.INFO, "{0} tasks received, but skipping because not optimizable.", numTasks);
      }
    }
  }
}
