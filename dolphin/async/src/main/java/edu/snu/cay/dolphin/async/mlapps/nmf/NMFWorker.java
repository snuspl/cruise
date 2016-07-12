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
package edu.snu.cay.dolphin.async.mlapps.nmf;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.snu.cay.common.metric.*;
import edu.snu.cay.common.metric.avro.Metrics;
import edu.snu.cay.dolphin.async.Worker;
import edu.snu.cay.dolphin.async.metric.Tracer;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorEntry;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetricsMsg;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static edu.snu.cay.dolphin.async.metric.WorkerConstants.KEY_WORKER_COMPUTE_TIME;
import static edu.snu.cay.dolphin.async.mlapps.nmf.NMFParameters.*;

/**
 * Worker for non-negative matrix factorization via SGD.
 *
 * Assumes that indices in {@link NMFData} are one-based.
 */
final class NMFWorker implements Worker {

  private static final Logger LOG = Logger.getLogger(NMFWorker.class.getName());

  private final ParameterWorker<Integer, Vector, Vector> parameterWorker;
  private final VectorFactory vectorFactory;
  private final NMFDataParser dataParser;
  private final int rank;
  private final double stepSize;
  private final double lambda;
  /**
   * Mini-batch size used for mini-batch gradient descent.
   * If less than {@code 1}, a standard gradient descent method is used.
   */
  private final int batchSize;
  private final boolean printMatrices;
  private final int logPeriod;
  private final NMFModelGenerator modelGenerator;
  private final Map<Integer, Vector> rMatrix; // R matrix cache
  private final Map<Integer, Vector> gradients; // R matrix gradients

  private final DataIdFactory<Long> idFactory;
  private final MemoryStore<Long> memoryStore;

  // TODO #487: Metric collecting should be done by the system, not manually by the user code.
  private final MetricsCollector metricsCollector;
  private final InsertableMetricTracker insertableMetricTracker;
  private final MetricsHandler metricsHandler;
  private final MetricsMsgSender<WorkerMetricsMsg> metricsMsgSender;

  private final Tracer pushTracer;
  private final Tracer pullTracer;
  private final Tracer computeTracer;

  /**
   * Number of iterations.
   */
  private int iteration = 0;

  @Inject
  private NMFWorker(final NMFDataParser dataParser,
                    final ParameterWorker<Integer, Vector, Vector> parameterWorker,
                    final VectorFactory vectorFactory,
                    @Parameter(Rank.class) final int rank,
                    @Parameter(StepSize.class) final double stepSize,
                    @Parameter(Lambda.class) final double lambda,
                    @Parameter(BatchSize.class) final int batchSize,
                    @Parameter(PrintMatrices.class) final boolean printMatrices,
                    @Parameter(LogPeriod.class) final int logPeriod,
                    final NMFModelGenerator modelGenerator,
                    final DataIdFactory<Long> idFactory,
                    final MemoryStore<Long> memoryStore,
                    final MetricsCollector metricsCollector,
                    final InsertableMetricTracker insertableMetricTracker,
                    final MetricsHandler metricsHandler,
                    final MetricsMsgSender<WorkerMetricsMsg> metricsMsgSender) {
    this.parameterWorker = parameterWorker;
    this.vectorFactory = vectorFactory;
    this.dataParser = dataParser;
    this.rank = rank;
    this.stepSize = stepSize;
    this.lambda = lambda;
    this.batchSize = batchSize;
    this.printMatrices = printMatrices;
    this.logPeriod = logPeriod;
    this.modelGenerator = modelGenerator;
    this.idFactory = idFactory;
    this.memoryStore = memoryStore;
    this.metricsCollector = metricsCollector;
    this.insertableMetricTracker = insertableMetricTracker;
    this.metricsHandler = metricsHandler;
    this.metricsMsgSender = metricsMsgSender;

    this.rMatrix = Maps.newHashMap();
    this.gradients = Maps.newHashMap();

    this.pushTracer = new Tracer();
    this.pullTracer = new Tracer();
    this.computeTracer = new Tracer();
  }

  @Override
  public void initialize() {
    final Set<MetricTracker> metricTrackerSet = new HashSet<>(1);
    metricTrackerSet.add(insertableMetricTracker);
    metricsCollector.registerTrackers(metricTrackerSet);

    final List<NMFData> dataValues = dataParser.parse();
    final List<Long> dataKeys;

    try {
      dataKeys = idFactory.getIds(dataValues.size());
    } catch (final IdGenerationException e) {
      throw new RuntimeException(e);
    }

    memoryStore.putList(dataKeys, dataValues);

    final Set<Integer> keys = new HashSet<>();
    // aggregate column indices
    for (final NMFData datum : dataValues) {
      for (final Pair<Integer, Double> column : datum.getColumns()) {
        keys.add(column.getFirst());
      }
    }

    LOG.log(Level.INFO, "Step size = {0}", stepSize);
    LOG.log(Level.INFO, "Batch size = {0}", batchSize);
    LOG.log(Level.INFO, "Total number of keys = {0}", keys.size());
    LOG.log(Level.INFO, "Total number of input rows = {0}", dataValues.size());
  }

  /**
   * @param dataValues Dataset assigned to this worker
   * @return Keys to send pull requests, which are determined by ratings of NMFData.
   */
  private List<Integer> getKeys(final Collection<NMFData> dataValues) {
    final List<Integer> keys = new ArrayList<>();
    final Set<Integer> keySet = Sets.newTreeSet();
    // aggregate column indices
    for (final NMFData datum : dataValues) {
      keySet.addAll(
          datum.getColumns()
              .stream()
              .map(Pair::getFirst)
              .collect(Collectors.toList()));
    }
    return keys;

  }

  private void saveRMatrixGradient(final int key, final Vector newGrad) {
    final Vector grad = gradients.get(key);
    if (grad == null) {
      // l2 regularization term. 2 * lambda * R_{*, j}
      if (lambda != 0.0D) {
        newGrad.axpy(2.0D * lambda, rMatrix.get(key));
      }
      gradients.put(key, newGrad);
    } else {
      grad.addi(newGrad);
    }
  }

  private void pushAndClearGradients() {
    // push gradients
    pushTracer.startTimer();
    for (final Map.Entry<Integer, Vector> entry : gradients.entrySet()) {
      parameterWorker.push(entry.getKey(), entry.getValue());
    }
    pushTracer.recordTime(gradients.size());
    // clear gradients
    gradients.clear();
  }

  private void pullRMatrix(final List<Integer> pKeys) {
    pullTracer.startTimer();
    final List<Vector> vectors = parameterWorker.pull(pKeys);
    for (int i = 0; i < pKeys.size(); ++i) {
      rMatrix.put(pKeys.get(i), vectors.get(i));
    }
    pullTracer.recordTime(pKeys.size());
  }

  private void resetTracers() {
    pushTracer.resetTrace();
    pullTracer.resetTrace();
    computeTracer.resetTrace();
  }

  private void sendMetrics(final int numDataBlocks) {
    try {
      insertableMetricTracker.put(KEY_WORKER_COMPUTE_TIME, computeTracer.totalElapsedTime());
      metricsCollector.stop();
      final Metrics metrics = metricsHandler.getMetrics();
      final WorkerMetricsMsg metricsMessage = WorkerMetricsMsg.newBuilder()
          .setMetrics(metrics)
          .setIteration(iteration)
          .setNumDataBlocks(numDataBlocks)
          .build();
      LOG.log(Level.INFO, "Sending metricsMessage {0}", metricsMessage);

      metricsMsgSender.send(metricsMessage);
    } catch (final MetricException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    try {
      metricsCollector.start();
    } catch (final MetricException e) {
      throw new RuntimeException(e);
    }

    ++iteration;
    final long iterationBegin = System.currentTimeMillis();
    double lossSum = 0.0;
    int elemCount = 0;
    int rowCount = 0;
    resetTracers();

    final Map<Long, NMFData> workloadMap = memoryStore.getAll();
    final Collection<NMFData> workload = workloadMap.values();

    pullRMatrix(getKeys(workload));

    for (final NMFData datum : workload) {
      computeTracer.startTimer();
      final Vector lVec = datum.getVector(); // L_{i, *} : i-th row of L
      final Vector lGradSum;
      if (lambda != 0.0D) {
        // l2 regularization term. 2 * lambda * L_{i, *}
        lGradSum = lVec.scale(2.0D * lambda);
      } else {
        lGradSum = vectorFactory.createDenseZeros(rank);
      }

      for (final Pair<Integer, Double> column : datum.getColumns()) { // a pair of column index and value
        final int colIdx = column.getFirst();
        final Vector rVec = rMatrix.get(colIdx); // R_{*, j} : j-th column of R
        final double error = lVec.dot(rVec) - column.getSecond(); // e = L_{i, *} * R_{*, j} - D_{i, j}

        // compute gradients
        // lGrad = 2 * e * R_{*, j}'
        // rGrad = 2 * e * L_{i, *}'
        final Vector lGrad;
        final Vector rGrad;
        if (batchSize > 0) {
          lGrad = rVec.scale(2.0D * error / batchSize);
          rGrad = lVec.scale(2.0D * error / batchSize);
        } else {
          lGrad = rVec.scale(2.0D * error);
          rGrad = lVec.scale(2.0D * error);
        }

        // aggregate L matrix gradients
        lGradSum.addi(lGrad);

        // save R matrix gradients
        saveRMatrixGradient(colIdx, rGrad);

        // aggregate loss
        lossSum += error * error;
        ++elemCount;
      }

      // update L matrix
      modelGenerator.getValidVector(lVec.axpy(-stepSize, lGradSum));

      ++rowCount;
      computeTracer.recordTime(datum.getColumns().size());

      if (batchSize > 0 && rowCount % batchSize == 0) {
        pushAndClearGradients();
        pullRMatrix(getKeys(workload));
      }

      if (logPeriod > 0 && rowCount % logPeriod == 0) {
        final double elapsedTime = (System.currentTimeMillis() - iterationBegin) / 1000.0D;
        LOG.log(Level.INFO, "Iteration: {0}, Row Count: {1}, Avg Loss: {2}, Sum Loss : {3}, " +
            "Avg Comp Per Row: {4}, Sum Comp: {5}, Avg Pull: {6}, Sum Pull: {7}, Avg Push: {8}, " +
            "Sum Push: {9}, DvT: {10}, RvT: {11}, Elapsed Time: {12}",
            new Object[]{iteration, rowCount, String.format("%g", lossSum / elemCount), String.format("%g", lossSum),
                computeTracer.avgTimePerRecord(), computeTracer.totalElapsedTime(), pullTracer.avgTimePerRecord(),
                pullTracer.totalElapsedTime(), pushTracer.avgTimePerRecord(),
                pushTracer.totalElapsedTime(), elemCount / elapsedTime, rowCount / elapsedTime, elapsedTime});
      }
    }

    pushAndClearGradients();

    final double elapsedTime = (System.currentTimeMillis() - iterationBegin) / 1000.0D;
    LOG.log(Level.INFO, "End Iteration: {0}, Row Count: {1}, Avg Loss: {2}, Sum Loss : {3}, " +
            "Avg Comp Per Row: {4}, Sum Comp: {5}, Avg Pull: {6}, Sum Pull: {7}, Avg Push: {8}, " +
            "Sum Push: {9}, DvT: {10}, RvT: {11}, Elapsed Time: {12}",
        new Object[]{iteration, rowCount, String.format("%g", lossSum / elemCount), String.format("%g", lossSum),
            computeTracer.avgTimePerRecord(), computeTracer.totalElapsedTime(), pullTracer.avgTimePerRecord(),
            pullTracer.totalElapsedTime(), pushTracer.avgTimePerRecord(),
            pushTracer.totalElapsedTime(), elemCount / elapsedTime, rowCount / elapsedTime, elapsedTime});

    sendMetrics(memoryStore.getNumBlocks());
  }

  @Override
  public void cleanup() {
    try {
      metricsCollector.close();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    // print generated matrices
    if (!printMatrices) {
      return;
    }
    // print L matrix
    final Map<Long, NMFData> workloadMap = memoryStore.getAll();
    final Collection<NMFData> workload = workloadMap.values();

    final StringBuilder lsb = new StringBuilder();
    for (final NMFData datum : workload) {
      lsb.append(String.format("L(%d, *):", datum.getRowIndex()));
      for (final VectorEntry valueEntry : datum.getVector()) {
        lsb.append(' ');
        lsb.append(valueEntry.value());
      }
      lsb.append('\n');
    }
    LOG.log(Level.INFO, lsb.toString());

    // print transposed R matrix
    pullRMatrix(getKeys(workload));
    final StringBuilder rsb = new StringBuilder();
    for (final Map.Entry<Integer, Vector> entry : rMatrix.entrySet()) {
      rsb.append(String.format("R(*, %d):", entry.getKey()));
      for (final VectorEntry valueEntry : entry.getValue()) {
        rsb.append(' ');
        rsb.append(valueEntry.value());
      }
      rsb.append('\n');
    }
    LOG.log(Level.INFO, rsb.toString());
  }
}
