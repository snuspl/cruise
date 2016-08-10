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
  private volatile Map<String, Integer> numBlockByEvalIdForWorker;
  private volatile Map<String, Integer> numBlockByEvalIdForServer;


  @Inject
  private MetricManager() {
    this.workerEvalParams = Collections.synchronizedMap(new HashMap<>());
    this.serverEvalParams = Collections.synchronizedMap(new HashMap<>());
    this.metricCollectionEnabled = false;
    this.numBlockByEvalIdForWorker = null;
    this.numBlockByEvalIdForServer = null;
  }

  /**
   * Store a {@link EvaluatorParameters} object for a parameter set of a certain worker.
   * This method does not override existing metrics with the same {@code workerId}.
   * Instead, a new {@link EvaluatorParameters} object is allocated for each call.
   */
  public void storeWorkerMetrics(final String workerId, final WorkerMetrics metrics) {
    if (metricCollectionEnabled) {
      final int numDataBlocksOnWorker = metrics.getNumDataBlocks();

      if (numBlockByEvalIdForWorker != null && numBlockByEvalIdForWorker.containsKey(workerId)) {
        final int numDataBlocksOnDriver = numBlockByEvalIdForWorker.get(workerId);

        if (numDataBlocksOnWorker == numDataBlocksOnDriver) {
          final DataInfo dataInfo = new DataInfoImpl(numDataBlocksOnWorker);
          final EvaluatorParameters evaluatorParameters = new WorkerEvaluatorParameters(workerId, dataInfo, metrics);
          synchronized (workerEvalParams) {
            if (!workerEvalParams.containsKey(workerId)) {
              workerEvalParams.put(workerId, new ArrayList<>());
            }
            workerEvalParams.get(workerId).add(evaluatorParameters);
          }
        } else {
          LOG.log(Level.FINE, "{0} contains {1} blocks, driver says {2} blocks. Dropping metric.",
              new Object[]{workerId, numDataBlocksOnWorker, numDataBlocksOnDriver});
        }
      } else {
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
      final int numModelBlocksOnServer = metrics.getNumModelBlocks();

      if (numBlockByEvalIdForServer != null && numBlockByEvalIdForServer.containsKey(serverId)) {
        final int numModelBlocksOnDriver = numBlockByEvalIdForServer.get(serverId);

        if (numModelBlocksOnServer == numModelBlocksOnDriver) {
          final DataInfo dataInfo = new DataInfoImpl(numModelBlocksOnServer);
          final EvaluatorParameters evaluatorParameters = new ServerEvaluatorParameters(serverId, dataInfo, metrics);
          synchronized (serverEvalParams) {
            if (!serverEvalParams.containsKey(serverId)) {
              serverEvalParams.put(serverId, new ArrayList<>());
            }
            serverEvalParams.get(serverId).add(evaluatorParameters);
          }
        } else {
          LOG.log(Level.FINE, "{0} contains {1} blocks, driver says {2} blocks. Dropping metric.",
              new Object[]{serverId, numModelBlocksOnServer, numModelBlocksOnDriver});
        }
      } else {
        LOG.log(Level.FINE, "No information about {0}. Dropping metric.", serverId);
      }
    } else {
      LOG.log(Level.FINE, "Metric collection disabled. Dropping metric from {0}", serverId);
    }
  }

  public Map<String, List<EvaluatorParameters>> getWorkerMetrics() {
    synchronized (workerEvalParams) {
      final Map<String, List<EvaluatorParameters>> currWorkerMetrics = new HashMap<>();

      for (final Map.Entry<String, List<EvaluatorParameters>> entry : workerEvalParams.entrySet()) {
        currWorkerMetrics.put(entry.getKey(), new ArrayList<>(entry.getValue()));
      }
      return currWorkerMetrics;
    }
  }

  public Map<String, List<EvaluatorParameters>> getServerMetrics() {
    synchronized (serverEvalParams) {
      final Map<String, List<EvaluatorParameters>> currServerMetrics = new HashMap<>();

      for (final Map.Entry<String, List<EvaluatorParameters>> entry : serverEvalParams.entrySet()) {
        currServerMetrics.put(entry.getKey(), new ArrayList<>(entry.getValue()));
      }
      return currServerMetrics;
    }
  }

  /**
   * Stops metric collection and clear metrics collected until this point.
   */
  public void stopMetricCollection() {
    LOG.log(Level.INFO, "Metric collection stopped!");
    metricCollectionEnabled = false;
    clearServerMetrics();
    clearWorkerMetrics();
  }

  /**
   * Starts metric collection and loads information required for metric validation.
   */
  public void startMetricCollection() {
    LOG.log(Level.INFO, "Metric collection started!");
    metricCollectionEnabled = true;
  }

  /**
   * Loads information required for metric validation.
   * Any information to be used for metric validation may be added here
   * and used to filter out invalid incoming metric in
   * {@link #storeWorkerMetrics(String, WorkerMetrics)} or {@link #storeServerMetrics(String, ServerMetrics)}
   */
  public void loadMetricValidationInfo(final Map<String, Integer> numBlockForWorker,
                                       final Map<String, Integer> numBlockForServer) {
    this.numBlockByEvalIdForWorker = numBlockForWorker;
    this.numBlockByEvalIdForServer = numBlockForServer;
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
}
