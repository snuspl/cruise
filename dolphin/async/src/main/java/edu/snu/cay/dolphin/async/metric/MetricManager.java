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
package edu.snu.cay.dolphin.async.metric;

import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.dolphin.async.dashboard.DashboardConnector;
import edu.snu.cay.dolphin.async.optimizer.ServerEvaluatorParameters;
import edu.snu.cay.dolphin.async.optimizer.WorkerEvaluatorParameters;
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
 * After storing each metric, it also sends the metric to Dashboard,
 * which visualizes the received metrics, using {@link DashboardConnector}.
 */
@DriverSide
public final class MetricManager {
  private static final Logger LOG = Logger.getLogger(MetricManager.class.getName());

  /**
   * Worker-side metrics for epochs, each in the form of (workerId, {@link EvaluatorParameters}) mapping.
   */
  private final Map<String, List<EvaluatorParameters>> workerEvalEpochParams;

  /**
   * Worker-side metrics for mini-batches, each in the form of (workerId, {@link EvaluatorParameters}) mapping.
   */
  private final Map<String, List<EvaluatorParameters>> workerEvalMiniBatchParams;

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

  /**
   * Connector for sending received metrics to Dashboard server.
   */
  private final DashboardConnector dashboardConnector;

  /**
   * Constructor of MetricManager.
   */
  @Inject
  private MetricManager(final DashboardConnector dashboardConnector) {
    this.workerEvalEpochParams = Collections.synchronizedMap(new HashMap<>());
    this.workerEvalMiniBatchParams = new HashMap<>();
    this.serverEvalParams = new HashMap<>();
    this.metricCollectionEnabled = false;
    this.numBlockByEvalIdForWorker = null;
    this.numBlockByEvalIdForServer = null;

    this.dashboardConnector = dashboardConnector;
  }

  /**
   * Store a {@link EvaluatorParameters} object for a parameter set of a certain worker.
   * This method does not override existing metrics with the same {@code workerId}.
   * Instead, a new {@link EvaluatorParameters} object is allocated for each call.
   */
  void storeWorkerMetrics(final String workerId, final WorkerMetrics metrics) {
    if (metricCollectionEnabled) {
      if (numBlockByEvalIdForWorker != null && numBlockByEvalIdForWorker.containsKey(workerId)) {
        final int numDataBlocks = numBlockByEvalIdForWorker.get(workerId);

        final DataInfo dataInfo = new DataInfoImpl(numDataBlocks);
        final EvaluatorParameters evaluatorParameters = new WorkerEvaluatorParameters(workerId, dataInfo, metrics);

        if (metrics.getMiniBatchIdx() == null) {
          synchronized (workerEvalEpochParams) {
            // skip the first epoch metric for the worker after metric collection has begun
            if (!workerEvalEpochParams.containsKey(workerId)) {
              workerEvalEpochParams.put(workerId, new ArrayList<>());
            } else {
              if (metrics.getNumDataBlocks() == numDataBlocks) {
                workerEvalEpochParams.get(workerId).add(evaluatorParameters);
              } else {
                LOG.log(Level.SEVERE, "Inconsistent NumDataBlocks: driver = {0}, {1} = {2}",
                    new Object[] {numDataBlocks, workerId, metrics.getNumDataBlocks()});
              }
            }
          }
        } else {
          synchronized (workerEvalMiniBatchParams) {
            // only collect the metric if the worker has completed its first epoch after metric collection has begun
            if (workerEvalEpochParams.containsKey(workerId)) {
              if (!workerEvalMiniBatchParams.containsKey(workerId)) {
                workerEvalMiniBatchParams.put(workerId, new ArrayList<>());
              }
              workerEvalMiniBatchParams.get(workerId).add(evaluatorParameters);
            }
          }
        }
      } else {
        LOG.log(Level.FINE, "No information about {0}. Dropping metric.", workerId);
      }
    } else {
      LOG.log(Level.FINE, "Metric collection disabled. Dropping metric from {0}", workerId);
    }

    dashboardConnector.sendWorkerMetric(workerId, metrics);
  }

  /**
   * Store a {@link EvaluatorParameters} object for a parameter set of a certain server.
   * This method does not override existing metrics with the same {@code serverId}.
   * Instead, a new {@link EvaluatorParameters} object is allocated for each call.
   */
  void storeServerMetrics(final String serverId, final ServerMetrics metrics) {
    if (metricCollectionEnabled) {
      if (numBlockByEvalIdForServer != null && numBlockByEvalIdForServer.containsKey(serverId)) {
        final int numModelBlocks = numBlockByEvalIdForServer.get(serverId);

        final DataInfo dataInfo = new DataInfoImpl(numModelBlocks);
        final EvaluatorParameters evaluatorParameters = new ServerEvaluatorParameters(serverId, dataInfo, metrics);
        synchronized (serverEvalParams) {
          // only collect the metric all workers have sent at least one metric after metric collection has begun
          if (workerEvalEpochParams.size() == numBlockByEvalIdForWorker.size()) {
            if (!serverEvalParams.containsKey(serverId)) {
              serverEvalParams.put(serverId, new ArrayList<>());
            }
            if (metrics.getNumModelBlocks() == numModelBlocks) {
              serverEvalParams.get(serverId).add(evaluatorParameters);
            } else {
              LOG.log(Level.SEVERE, "Inconsistent NumModelBlocks: driver = {0}, {1} = {2}",
                  new Object[] {numModelBlocks, serverId, metrics.getNumModelBlocks()});
            }
          }
        }
      } else {
        LOG.log(Level.FINE, "No information about {0}. Dropping metric.", serverId);
      }
    } else {
      LOG.log(Level.FINE, "Metric collection disabled. Dropping metric from {0}", serverId);
    }

    dashboardConnector.sendServerMetric(serverId, metrics);
  }

  public Map<String, List<EvaluatorParameters>> getWorkerMiniBatchMetrics() {
    return getMetrics(workerEvalMiniBatchParams);
  }

  public Map<String, List<EvaluatorParameters>> getWorkerEpochMetrics() {
    return getMetrics(workerEvalEpochParams);
  }

  public Map<String, List<EvaluatorParameters>> getServerMetrics() {
    return getMetrics(serverEvalParams);
  }

  private Map<String, List<EvaluatorParameters>> getMetrics(final Map<String, List<EvaluatorParameters>> evalParams) {
    synchronized (evalParams) {
      final Map<String, List<EvaluatorParameters>> currServerMetrics = new HashMap<>();

      for (final Map.Entry<String, List<EvaluatorParameters>> entry : evalParams.entrySet()) {
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
    synchronized (workerEvalMiniBatchParams) {
      workerEvalMiniBatchParams.clear();
    }
    synchronized (workerEvalEpochParams) {
      workerEvalEpochParams.clear();
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
