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

import edu.snu.cay.dolphin.async.metric.avro.ServerMetrics;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.dolphin.async.dashboard.DashboardConnector;
import edu.snu.cay.dolphin.async.optimizer.ServerEvaluatorParameters;
import edu.snu.cay.dolphin.async.optimizer.WorkerEvaluatorParameters;
import edu.snu.cay.dolphin.async.optimizer.api.EvaluatorParameters;
import edu.snu.cay.dolphin.async.optimizer.impl.DataInfoImpl;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A manager class that handles metrics from workers and servers.
 * It stores only valid metrics and provides them to optimization process.
 * It also sends the metric to Dashboard,
 * which visualizes the received metrics, using {@link DashboardConnector}.
 */
@DriverSide
@ThreadSafe
public final class MetricManager {
  private static final Logger LOG = Logger.getLogger(MetricManager.class.getName());

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
   * A class for storing metrics.
   */
  private final MetricStore metricStore;

  /**
   * Connector for sending received metrics to Dashboard server.
   */
  private final DashboardConnector dashboardConnector;

  /**
   * Constructor of MetricManager.
   */
  @Inject
  private MetricManager(final DashboardConnector dashboardConnector) {
    this.metricCollectionEnabled = false;
    this.numBlockByEvalIdForWorker = null;
    this.numBlockByEvalIdForServer = null;

    this.metricStore = new MetricStore();

    this.dashboardConnector = dashboardConnector;
  }

  /**
   * Store a {@link EvaluatorParameters} object for a parameter set of a certain worker.
   * This method does not override existing metrics with the same {@code workerId}.
   * Instead, a new {@link EvaluatorParameters} object is allocated for each call.
   */
  void storeWorkerMetrics(final String workerId, final WorkerMetrics metrics) {
    if (metricCollectionEnabled) {
      if (isValidSource(workerId, numBlockByEvalIdForWorker)) {
        final int numDataBlocks = numBlockByEvalIdForWorker.get(workerId);

        final WorkerEvaluatorParameters evaluatorParameters =
            new WorkerEvaluatorParameters(workerId, new DataInfoImpl(numDataBlocks), metrics);

        if (metrics.getMiniBatchIdx() == null) {
          metricStore.storeWorkerEpochMetrics(workerId, evaluatorParameters);
        } else {
          metricStore.storeWorkerMiniBatchMetrics(workerId, evaluatorParameters);
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
      if (isValidSource(serverId, numBlockByEvalIdForServer)) {
        final int numModelBlocks = numBlockByEvalIdForServer.get(serverId);

        final ServerEvaluatorParameters evaluatorParameters =
            new ServerEvaluatorParameters(serverId, new DataInfoImpl(numModelBlocks), metrics);

        metricStore.storeServerMetrics(serverId, evaluatorParameters);

      } else {
        LOG.log(Level.FINE, "No information about {0}. Dropping metric.", serverId);
      }
    } else {
      LOG.log(Level.FINE, "Metric collection disabled. Dropping metric from {0}", serverId);
    }

    dashboardConnector.sendServerMetric(serverId, metrics);
  }

  /**
   * Checks whether the source of the metrics exists in the {@link edu.snu.cay.services.em.driver.api.EMMaster}'s
   * evaluator list.
   * @param srcId id of the source evaluator that sent the metrics.
   * @param validationInfo a map consists of evaluator ids as its keys
   * @return {@code true} if the source exists in the {@link edu.snu.cay.services.em.driver.api.EMMaster}'s view.
   */
  private boolean isValidSource(final String srcId, final Map<String, Integer> validationInfo) {
    return validationInfo != null && validationInfo.containsKey(srcId);
  }

  /**
   * Returns a map that contains mini-batch metrics from workers.
   * Key is a worker id and value is a list of {@link EvaluatorParameters} from the corresponding worker.
   * @return a map between worker id and its mini-batch metrics
   */
  public Map<String, List<EvaluatorParameters>> getWorkerMiniBatchMetrics() {
    return copyMetrics(metricStore.getWorkerMiniBatchMetrics());
  }

  /**
   * Returns a map that contains epoch metrics from workers.
   * Key is a worker id and value is a list of {@link EvaluatorParameters} from the corresponding worker.
   * The list of a worker is empty, when the worker still does not report valid epoch metrics.
   * @return a map between worker id and its epoch metrics
   */
  public Map<String, List<EvaluatorParameters>> getWorkerEpochMetrics() {
    return copyMetrics(metricStore.getWorkerEpochMetrics());
  }

  /**
   * Returns a map that contains metrics from servers.
   * Key is a server id and value is a list of {@link EvaluatorParameters} from the corresponding server.
   * @return a map between server id and its metrics
   */
  public Map<String, List<EvaluatorParameters>> getServerMetrics() {
    return copyMetrics(metricStore.getServerMetrics());
  }

  private Map<String, List<EvaluatorParameters>> copyMetrics(final Map<String, List<EvaluatorParameters>> evalParams) {
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
    metricStore.clearServerMetrics();
    metricStore.clearWorkerMetrics();
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
    LOG.log(Level.INFO, "Load validation info. Worker-side partition info: {0}, Server-side partition info: {1}",
        new Object[]{numBlockForWorker, numBlockForServer});
    this.numBlockByEvalIdForWorker = numBlockForWorker;
    this.numBlockByEvalIdForServer = numBlockForServer;
  }

  /**
   * A class that stores metrics from workers and servers.
   */
  private final class MetricStore {

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

    private MetricStore() {
      this.workerEvalEpochParams = Collections.synchronizedMap(new HashMap<>());
      this.workerEvalMiniBatchParams = Collections.synchronizedMap(new HashMap<>());
      this.serverEvalParams = Collections.synchronizedMap(new HashMap<>());
    }

    private void storeWorkerEpochMetrics(final String workerId, final WorkerEvaluatorParameters evalParams) {
      synchronized (workerEvalEpochParams) {
        // skip the first epoch metric for the worker after metric collection has begun
        if (!workerEvalEpochParams.containsKey(workerId)) {
          workerEvalEpochParams.put(workerId, new ArrayList<>());
        } else {
          if (isValidNumBlocks(evalParams.getMetrics().getNumDataBlocks(), evalParams)) {
            workerEvalEpochParams.get(workerId).add(evalParams);
          }
        }
      }
    }

    private void storeWorkerMiniBatchMetrics(final String workerId, final EvaluatorParameters evalParams) {
      // only collect the metric if the worker has completed its first epoch after metric collection has begun
      if (workerEvalEpochParams.containsKey(workerId)) {
        synchronized (workerEvalMiniBatchParams) {
          if (!workerEvalMiniBatchParams.containsKey(workerId)) {
            workerEvalMiniBatchParams.put(workerId, new ArrayList<>());
          }
          workerEvalMiniBatchParams.get(workerId).add(evalParams);
        }
      }
    }

    private void storeServerMetrics(final String serverId, final ServerEvaluatorParameters evalParams) {
      if (workerEvalEpochParams.size() == numBlockByEvalIdForWorker.size()) {
        synchronized (serverEvalParams) {
          // only collect the metric all workers have sent at least one metric after metric collection has begun
          if (!serverEvalParams.containsKey(serverId)) {
            serverEvalParams.put(serverId, new ArrayList<>());
          }
          if (isValidNumBlocks(evalParams.getMetrics().getNumModelBlocks(), evalParams)) {
            serverEvalParams.get(serverId).add(evalParams);
          }
        }
      }
    }

    /**
     * Check whether the number of blocks matches with master's view.
     * @param numBlocks the number of blocks that source evaluator is supposed to have.
     * @param evalParams the Metric's information including the number of blocks.
     * @return {@code true} if the number of blocks matches
     */
    private boolean isValidNumBlocks(final int numBlocks, final EvaluatorParameters evalParams) {
      if (numBlocks == evalParams.getDataInfo().getNumBlocks()) {
        return true;
      } else {
        LOG.log(Level.SEVERE, "Inconsistent NumBlocks: driver = {0}, {1} = {2}",
            new Object[] {evalParams.getDataInfo().getNumBlocks(), evalParams.getId(), numBlocks});
        return false;
      }
    }

    private Map<String, List<EvaluatorParameters>> getWorkerEpochMetrics() {
      return workerEvalEpochParams;
    }

    private Map<String, List<EvaluatorParameters>> getWorkerMiniBatchMetrics() {
      return workerEvalMiniBatchParams;
    }

    private Map<String, List<EvaluatorParameters>> getServerMetrics() {
      return serverEvalParams;
    }

    /**
     * Empty out the current set of worker metrics.
     */
    private void clearWorkerMetrics() {
      workerEvalMiniBatchParams.clear();
      workerEvalEpochParams.clear();
    }

    /**
     * Empty out the current set of server metrics.
     */
    private void clearServerMetrics() {
      serverEvalParams.clear();
    }
  }
}
