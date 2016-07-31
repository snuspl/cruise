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
import edu.snu.cay.services.em.driver.impl.BlockManager;
import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;
import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A temporary storage for holding worker and server metrics related to optimization.
 */
@DriverSide
public final class MetricManager {
  private static final Logger LOG = Logger.getLogger(MetricManager.class.getName());
  private final BlockManager blockManager;

  /**
   * Worker-side metrics, each in the form of (workerId, {@link EvaluatorParameters}) mapping.
   */
  private final Map<String, List<EvaluatorParameters>> workerEvalParams;

  /**
   * Server-side metrics, each in the form of (serverId, {@link EvaluatorParameters}) mapping.
   */
  private final Map<String, List<EvaluatorParameters>> serverEvalParams;

  /**
   * A flag to enable/disable metric collection. It is disabled by default.
   */
  private boolean metricCollectionEnabled;

  /**
   * A map that contains each evaluator's mapping to the number of blocks it contains.
   * The map is loaded only when metric collection is enabled.
   */
  private Map<String, Integer> numBlockOwnershipByEvalId;

  @Inject
  private MetricManager(final BlockManager blockManager) {
    this.blockManager = blockManager;
    this.workerEvalParams = Collections.synchronizedMap(new HashMap<>());
    this.serverEvalParams = Collections.synchronizedMap(new HashMap<>());
    this.metricCollectionEnabled = false;
    this.numBlockOwnershipByEvalId = null;
  }

  /**
   * Store a {@link EvaluatorParameters} object for a parameter set of a certain worker.
   * This method does not override existing metrics with the same {@code workerId}.
   * Instead, a new {@link EvaluatorParameters} object is allocated for each call.
   */
  public void storeWorkerMetrics(final String workerId, final WorkerMetrics metrics) {
    if (metricCollectionEnabled) {
      try {
        final int numDataBlocksOnWorker = metrics.getNumDataBlocks();
        final int numDataBlocksOnDriver = numBlockOwnershipByEvalId.get(workerId);

        if (numDataBlocksOnWorker == numDataBlocksOnDriver) {
          final DataInfo dataInfo = new DataInfoImpl(numDataBlocksOnWorker);
          final EvaluatorParameters evaluatorParameters = new WorkerEvaluatorParameters(workerId, dataInfo, metrics);
          workerEvalParams.computeIfAbsent(workerId, metricList -> new ArrayList<>()).add(evaluatorParameters);
        } else {
          LOG.log(Level.FINE, "{0} contains {1} blocks, driver says {2} blocks. Dropping metric.",
              new Object[]{workerId, numDataBlocksOnWorker, numDataBlocksOnDriver});
        }
      } catch (NullPointerException e) {
        // NullPointerException thrown if {@code numBlockOwnershipByEvalId} does not contain a mapping
        // i.e. the metric from {@code workerId} is from an unknown evaluator (probably a deleted worker)
        LOG.log(Level.FINE, "No information about {0}. Dropping metric.", workerId);
      }

    } else {
      LOG.log(Level.FINE, "Metric collection disabled. Dropping metric from {0}", workerId);
    }
  }

  /**
   * Store a {@link EvaluatorParameters} object for a parameter set of a certain server.
   * This method does not override existing metrics with the same {@code serverId}.
   * Instead, a new {@link EvaluatorParameters} object is allocated for each call.
   */
  public void storeServerMetrics(final String serverId, final ServerMetrics metrics) {
    if (metricCollectionEnabled) {
      try {
        final int numDataBlocksOnServer = metrics.getNumModelParamBlocks();
        final int numDataBlocksOnDriver = numBlockOwnershipByEvalId.get(serverId);

        if (numDataBlocksOnServer == numDataBlocksOnDriver) {
          final DataInfo dataInfo = new DataInfoImpl(numDataBlocksOnServer);
          final EvaluatorParameters evaluatorParameters = new ServerEvaluatorParameters(serverId, dataInfo, metrics);
          serverEvalParams.computeIfAbsent(serverId, metricList -> new ArrayList<>()).add(evaluatorParameters);
        } else {
          LOG.log(Level.FINE, "{0} contains {1} blocks, driver says {2} blocks. Dropping metric.",
              new Object[]{serverId, numDataBlocksOnServer, numDataBlocksOnDriver});
        }
      } catch (NullPointerException e) {
        // NullPointerException thrown if {@code numBlockOwnershipByEvalId} does not contain a mapping
        // i.e. the metric from {@code serverId} is from an unknown evaluator (probably a deleted server)
        LOG.log(Level.FINE, "No information about {0}. Dropping metric.", serverId);
      }
    } else {
      LOG.log(Level.FINE, "Metric collection disabled. Dropping metric from {0}", serverId);
    }
  }

  public Map<String, List<EvaluatorParameters>> getWorkerMetrics() {
    synchronized (workerEvalParams) {
      final Map<String, List<EvaluatorParameters>> currWorkerMetrics = new HashMap<>(workerEvalParams);
      return currWorkerMetrics;
    }
  }

  public Map<String, List<EvaluatorParameters>> getServerMetrics() {
    synchronized (serverEvalParams) {
      final Map<String, List<EvaluatorParameters>> currServerMetrics = new HashMap<>(serverEvalParams);
      return currServerMetrics;
    }
  }

  /**
   * Stops metric collection and clear metrics collected until this point.
   */
  public void stopMetricCollection() {
    metricCollectionEnabled = false;
    clearServerMetrics();
    clearWorkerMetrics();
  }

  /**
   * Starts metric collection and loads information required for metric validation.
   */
  public void startMetricCollection() {
    metricCollectionEnabled = true;
    loadMetricValidationInfo();
  }

  /**
   * Empty out the current set of worker metrics.
   */
  private void clearWorkerMetrics() {
    synchronized (workerEvalParams) {
      workerEvalParams.clear();
    }
  }

  /**
   * Empty out the current set of server metrics.
   */
  private void clearServerMetrics() {
    synchronized (serverEvalParams) {
      serverEvalParams.clear();
    }
  }

  /**
   * Loads information required for metric validation when metric collection is enabled.
   * Any information to be used for metric validation may be added here
   * and used to filter out invalid incoming metric in
   * {@link #storeWorkerMetrics(String, WorkerMetrics)} or {@link #storeServerMetrics(String, ServerMetrics)}
   */
  private void loadMetricValidationInfo() {
    numBlockOwnershipByEvalId = blockManager.getEvalIdToNumBlocks();
  }
}
