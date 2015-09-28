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
  private final Map<String, List<DataInfo>> idToDataInfos;
  private final Map<String, Map<String, Double>> idToMetrics;
  private String controllerId;
  private Map<String, Double> controllerMetrics;

  private int numComputeTasks = Integer.MAX_VALUE;
  private int numReceived = 0;

  /**
   * @param optimizationOrchestrator the optimization orchestrator that instantiates this instance
   */
  public MetricsReceiver(final OptimizationOrchestrator optimizationOrchestrator) {
    this.optimizationOrchestrator = optimizationOrchestrator;
    this.idToMetrics = new HashMap<>();
    this.idToDataInfos = new HashMap<>();
  }

  public synchronized void addCompute(final String contextId,
                                      final Map<String, Double> metrics,
                                      final List<DataInfo> dataInfos) {
    idToMetrics.put(contextId, metrics);
    idToDataInfos.put(contextId, dataInfos);
    numReceived++;
    LOG.log(Level.INFO, "numReceived " + numReceived);

    if (isReceivedAll()) {
      optimizationOrchestrator.run(idToDataInfos, idToMetrics, controllerId, controllerMetrics);
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
      optimizationOrchestrator.run(idToDataInfos, idToMetrics, controllerId, controllerMetrics);
    }
  }

  private boolean isReceivedAll() {
    return numReceived == numComputeTasks;
  }
}
