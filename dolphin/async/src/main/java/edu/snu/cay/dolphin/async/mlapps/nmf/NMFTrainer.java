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
import edu.snu.cay.common.metric.MetricsMsgSender;
import edu.snu.cay.common.metric.avro.Metrics;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.Trainer;
import edu.snu.cay.dolphin.async.metric.Tracer;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorEntry;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
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

import static edu.snu.cay.dolphin.async.mlapps.nmf.NMFParameters.*;

/**
 * Trainer for non-negative matrix factorization via SGD.
 *
 * Assumes that indices in {@link NMFData} are one-based.
 */
final class NMFTrainer implements Trainer {

  private static final Logger LOG = Logger.getLogger(NMFTrainer.class.getName());

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
  private final int numMiniBatchPerIter;
  private final boolean printMatrices;
  private final NMFModelGenerator modelGenerator;
  private final Map<Integer, Vector> rMatrix; // R matrix cache
  private final Map<Integer, Vector> gradients; // R matrix gradients

  private final DataIdFactory<Long> idFactory;
  private final MemoryStore<Long> memoryStore;

  // TODO #487: Metric collecting should be done by the system, not manually by the user code.
  private final MetricsMsgSender<WorkerMetrics> metricsMsgSender;

  private final Tracer pushTracer;
  private final Tracer pullTracer;
  private final Tracer computeTracer;

  @Inject
  private NMFTrainer(final NMFDataParser dataParser,
                     final ParameterWorker<Integer, Vector, Vector> parameterWorker,
                     final VectorFactory vectorFactory,
                     @Parameter(Rank.class) final int rank,
                     @Parameter(StepSize.class) final double stepSize,
                     @Parameter(Lambda.class) final double lambda,
                     @Parameter(Parameters.MiniBatches.class) final int numMiniBatchPerIter,
                     @Parameter(PrintMatrices.class) final boolean printMatrices,
                     final NMFModelGenerator modelGenerator,
                     final DataIdFactory<Long> idFactory,
                     final MemoryStore<Long> memoryStore,
                     final MetricsMsgSender<WorkerMetrics> metricsMsgSender) {
    this.parameterWorker = parameterWorker;
    this.vectorFactory = vectorFactory;
    this.dataParser = dataParser;
    this.rank = rank;
    this.stepSize = stepSize;
    this.lambda = lambda;
    this.numMiniBatchPerIter = numMiniBatchPerIter;
    this.printMatrices = printMatrices;
    this.modelGenerator = modelGenerator;
    this.idFactory = idFactory;
    this.memoryStore = memoryStore;
    this.metricsMsgSender = metricsMsgSender;

    this.rMatrix = Maps.newHashMap();
    this.gradients = Maps.newHashMap();

    this.pushTracer = new Tracer();
    this.pullTracer = new Tracer();
    this.computeTracer = new Tracer();
  }

  @Override
  public void initialize() {
    final List<NMFData> dataValues = dataParser.parse();

    final List<Long> dataKeys;
    try {
      dataKeys = idFactory.getIds(dataValues.size());
    } catch (final IdGenerationException e) {
      throw new RuntimeException(e);
    }

    memoryStore.putList(dataKeys, dataValues);

    LOG.log(Level.INFO, "Step size = {0}", stepSize);
    LOG.log(Level.INFO, "Number of batches per iteration = {0}", numMiniBatchPerIter);
    LOG.log(Level.INFO, "Total number of keys = {0}", getKeys(dataValues).size());
    LOG.log(Level.INFO, "Total number of training data items = {0}", dataValues.size());
  }

  @Override
  public void run(final int iteration) {
    final long iterationBegin = System.currentTimeMillis();
    double lossSum = 0.0;
    int elemCount = 0;
    resetTracers();

    final Map<Long, NMFData> workloadMap = memoryStore.getAll();
    final Collection<NMFData> workload = workloadMap.values();

    // Record the number of EM data blocks at the beginning of this iteration
    // to filter out stale metrics for optimization
    final int numEMBlocks = memoryStore.getNumBlocks();

    int numInstances = 0;
    int batchIdx = 0;
    int batchSize = workload.size() / numMiniBatchPerIter +
        ((workload.size() % numMiniBatchPerIter > batchIdx) ? 1 : 0);

    pullRMatrix(getKeys(workload));

    computeTracer.startTimer();
    for (final NMFData datum : workload) {
      if (numInstances >= batchSize) {
        computeTracer.recordTime(numInstances);

        // push gradients and pull fresh models
        pushAndResetGradients();
        pullRMatrix(getKeys(workload));

        numInstances = 0;
        ++batchIdx;

        // Recalculate batchSize to take care of the (workload.size() % numMiniBatchPerIter) instances.
        batchSize = workload.size() / numMiniBatchPerIter +
            ((workload.size() % numMiniBatchPerIter > batchIdx) ? 1 : 0);
        computeTracer.startTimer();
      }

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

        lGrad = rVec.scale(2.0D * error);
        rGrad = lVec.scale(2.0D * error);

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

      ++numInstances;
    }

    computeTracer.recordTime(numInstances);

    pushAndResetGradients();

    final double elapsedTime = (System.currentTimeMillis() - iterationBegin) / 1000.0D;
    final Metrics appMetrics = buildAppMetrics(lossSum, elemCount, elapsedTime, workload.size());
    final WorkerMetrics workerMetrics =
        buildMetricsMsg(iteration, appMetrics, numEMBlocks, workload.size(), elapsedTime);

    LOG.log(Level.INFO, "WorkerMetrics {0}", workerMetrics);
    sendMetrics(workerMetrics);
  }

  @Override
  public void cleanup() {
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

  private void pullRMatrix(final List<Integer> keys) {
    pullTracer.startTimer();
    final List<Vector> vectors = parameterWorker.pull(keys);
    for (int i = 0; i < keys.size(); ++i) {
      rMatrix.put(keys.get(i), vectors.get(i));
    }
    pullTracer.recordTime(keys.size());
  }

  private void pushAndResetGradients() {
    // push gradients
    pushTracer.startTimer();
    for (final Map.Entry<Integer, Vector> entry : gradients.entrySet()) {
      parameterWorker.push(entry.getKey(), entry.getValue());
    }
    pushTracer.recordTime(gradients.size());
    // clear gradients
    gradients.clear();
  }

  /**
   * @param dataValues Dataset assigned to this worker
   * @return Keys to send pull requests, which are determined by existing columns in NMFData.
   */
  private List<Integer> getKeys(final Collection<NMFData> dataValues) {
    computeTracer.startTimer();
    final ArrayList<Integer> keys = new ArrayList<>();
    final Set<Integer> keySet = Sets.newTreeSet();
    // aggregate column indices
    for (final NMFData datum : dataValues) {
      keySet.addAll(
          datum.getColumns()
              .stream()
              .distinct()
              .map(Pair::getFirst)
              .collect(Collectors.toList()));
    }
    keys.ensureCapacity(keySet.size());
    keys.addAll(keySet);
    computeTracer.recordTime(0);
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

  private void resetTracers() {
    pushTracer.resetTrace();
    pullTracer.resetTrace();
    computeTracer.resetTrace();
  }

  private void sendMetrics(final WorkerMetrics workerMetrics) {
    LOG.log(Level.FINE, "Sending WorkerMetrics {0}", workerMetrics);

    metricsMsgSender.send(workerMetrics);
  }

  private WorkerMetrics buildMetricsMsg(final int iteration, final Metrics appMetrics, final int numDataBlocks,
                                        final int numProcessedDataItemCount, final double elapsedTime) {
    return WorkerMetrics.newBuilder()
        .setMetrics(appMetrics)
        .setItrIdx(iteration)
        .setNumMiniBatchPerItr(numMiniBatchPerIter)
        .setNumDataBlocks(numDataBlocks)
        .setProcessedDataItemCount(numProcessedDataItemCount)
        .setTotalTime(elapsedTime)
        .setTotalCompTime(computeTracer.totalElapsedTime())
        .setTotalPullTime(pullTracer.totalElapsedTime())
        .setAvgPullTime(pullTracer.avgTimePerRecord())
        .setTotalPushTime(pushTracer.totalElapsedTime())
        .setAvgPushTime(pushTracer.avgTimePerRecord())
        .setParameterWorkerMetrics(parameterWorker.buildParameterWorkerMetrics())
        .build();
  }

  private Metrics buildAppMetrics(final double lossSum, final long elemCount, final double elapsedTime,
                                  final int numInstances) {
    final Map<CharSequence, Double> appMetricMap = new HashMap<>();
    appMetricMap.put(NMFParameters.MetricKeys.AVG_LOSS, lossSum / elemCount);
    appMetricMap.put(NMFParameters.MetricKeys.SUM_LOSS, lossSum);
    appMetricMap.put(NMFParameters.MetricKeys.DVT, elemCount / elapsedTime);
    appMetricMap.put(NMFParameters.MetricKeys.RVT, numInstances / elapsedTime);

    return Metrics.newBuilder()
        .setData(appMetricMap)
        .build();
  }
}
