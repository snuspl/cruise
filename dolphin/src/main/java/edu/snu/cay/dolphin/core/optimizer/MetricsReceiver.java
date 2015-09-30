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

import edu.snu.cay.services.em.optimizer.api.DataInfo;

import java.util.HashMap;
import java.util.List;
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
   * The list of DataInfos for each compute task (keyed by the compute task's contextId).
   */
  private final Map<String, List<DataInfo>> computeIdToDataInfos;

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
   * The number of compute tasks that should be received.
   * This value is set when the *controller task* reports the number of compute tasks in the communication group.
   */
  private int numComputeTasks = Integer.MAX_VALUE;

  /**
   * The number of compute task metrics that have been received.
   * Optimization is started when this value reaches numComputeTasks.
   */
  private int numComputeTasksReceived = 0;

  /**
   * @param optimizationOrchestrator the optimization orchestrator that instantiates this instance
   */
  public MetricsReceiver(final OptimizationOrchestrator optimizationOrchestrator) {
    this.optimizationOrchestrator = optimizationOrchestrator;
    this.computeIdToMetrics = new HashMap<>();
    this.computeIdToDataInfos = new HashMap<>();
  }

  public synchronized void addCompute(final String contextId,
                                      final Map<String, Double> metrics,
                                      final List<DataInfo> dataInfos) {
    computeIdToMetrics.put(contextId, metrics);
    computeIdToDataInfos.put(contextId, dataInfos);
    numComputeTasksReceived++;
    LOG.log(Level.INFO, "numComputeTasksReceived " + numComputeTasksReceived);

    if (isReceivedAll()) {
      optimizationOrchestrator.run(computeIdToDataInfos, computeIdToMetrics, controllerId, controllerMetrics);
    }
  }

  public synchronized void addController(final String contextId,
                                         final Map<String, Double> metrics,
                                         final int reportedNumComputeTasks) {
    this.controllerId = contextId;
    this.controllerMetrics = metrics;
    this.numComputeTasks = reportedNumComputeTasks;
    LOG.log(Level.INFO, "numComputeTasks " + numComputeTasks);

    if (isReceivedAll()) {
      optimizationOrchestrator.run(computeIdToDataInfos, computeIdToMetrics, controllerId, controllerMetrics);
    }
  }

  private boolean isReceivedAll() {
    return numComputeTasksReceived == numComputeTasks;
  }
}
