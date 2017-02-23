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

import com.google.common.collect.Sets;
import edu.snu.cay.common.metric.MetricsMsgSender;
import edu.snu.cay.common.metric.avro.Metrics;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.EpochInfo;
import edu.snu.cay.dolphin.async.MiniBatchInfo;
import edu.snu.cay.dolphin.async.ModelAccessor;
import edu.snu.cay.dolphin.async.Trainer;
import edu.snu.cay.dolphin.async.metric.Tracer;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorEntry;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static edu.snu.cay.dolphin.async.mlapps.nmf.NMFParameters.*;

/**
 * Trainer for non-negative matrix factorization via SGD.
 *
 * Assumes that indices in {@link NMFData} are one-based.
 */
final class NMFTrainer implements Trainer<NMFData> {

  private static final Logger LOG = Logger.getLogger(NMFTrainer.class.getName());

  private final ParameterWorker<Integer, Vector, Vector> parameterWorker;
  private final VectorFactory vectorFactory;
  private final int rank;
  private double stepSize;
  private final double lambda;

  /**
   * Number of training data instances to be processed per mini-batch.
   */
  private final int miniBatchSize;

  private final boolean printMatrices;
  private final NMFModelGenerator modelGenerator;

  /**
   * The step size drops by this rate.
   */
  private final double decayRate;

  /**
   * The step size drops after every {@code decayPeriod} iterations pass.
   */
  private final int decayPeriod;

  private final MemoryStore<Long> memoryStore;

  /**
   * Executes the Trainer threads.
   */
  private final ExecutorService executor;

  /**
   * Number of Trainer threads that train concurrently.
   */
  private final int numTrainerThreads;

  /**
   * Allows to access and update the latest model.
   */
  private final ModelAccessor<NMFModel> modelAccessor;

  // TODO #487: Metric collecting should be done by the system, not manually by the user code.
  private final MetricsMsgSender<WorkerMetrics> metricsMsgSender;

  private final Tracer pushTracer;
  private final Tracer pullTracer;
  private final Tracer computeTracer;

  @Inject
  private NMFTrainer(final ParameterWorker<Integer, Vector, Vector> parameterWorker,
                     final VectorFactory vectorFactory,
                     @Parameter(Rank.class) final int rank,
                     @Parameter(StepSize.class) final double stepSize,
                     @Parameter(Lambda.class) final double lambda,
                     @Parameter(DecayRate.class) final double decayRate,
                     @Parameter(DecayPeriod.class) final int decayPeriod,
                     @Parameter(Parameters.MiniBatchSize.class) final int miniBatchSize,
                     @Parameter(PrintMatrices.class) final boolean printMatrices,
                     @Parameter(Parameters.NumTrainerThreads.class) final int numTrainerThreads,
                     final ModelAccessor<NMFModel> modelAccessor,
                     final NMFModelGenerator modelGenerator,
                     final MemoryStore<Long> memoryStore,
                     final MetricsMsgSender<WorkerMetrics> metricsMsgSender) {
    this.parameterWorker = parameterWorker;
    this.vectorFactory = vectorFactory;
    this.rank = rank;
    this.stepSize = stepSize;
    this.lambda = lambda;
    this.decayRate = decayRate;
    if (decayRate <= 0.0 || decayRate > 1.0) {
      throw new IllegalArgumentException("decay_rate must be larger than 0 and less than or equal to 1");
    }
    this.decayPeriod = decayPeriod;
    if (decayPeriod <= 0) {
      throw new IllegalArgumentException("decay_period must be a positive value");
    }
    this.miniBatchSize = miniBatchSize;
    this.printMatrices = printMatrices;
    this.modelGenerator = modelGenerator;
    this.memoryStore = memoryStore;

    this.modelAccessor = modelAccessor;
    this.numTrainerThreads = numTrainerThreads;
    this.executor = Executors.newFixedThreadPool(numTrainerThreads);

    this.metricsMsgSender = metricsMsgSender;
    this.pushTracer = new Tracer();
    this.pullTracer = new Tracer();
    this.computeTracer = new Tracer();

    LOG.log(Level.INFO, "Number of Trainer threads = {0}", numTrainerThreads);
    LOG.log(Level.INFO, "Step size = {0}", stepSize);
    LOG.log(Level.INFO, "Number of instances per mini-batch = {0}", miniBatchSize);
  }

  @Override
  public void initialize() {
  }

  @Override
  public void runMiniBatch(final Collection<NMFData> miniBatchData, final MiniBatchInfo miniBatchInfo) {
    final CountDownLatch latch = new CountDownLatch(numTrainerThreads);

    final BlockingQueue<NMFData> instances = new ArrayBlockingQueue<>(miniBatchSize);
    instances.addAll(miniBatchData);
    final int numInstancesToProcess = instances.size();

    resetTracers();
    final long miniBatchStartTime = System.currentTimeMillis();

    // pull data when mini-batch is started
    final List<Integer> keys = getKeys(instances);
    LOG.log(Level.INFO, "Total number of keys = {0}", keys.size());
    pullModels(keys);

    final List<Future<NMFModel>> futures = new ArrayList<>(numTrainerThreads);
    try {
      computeTracer.startTimer();

      // Threads drain multiple instances from shared queue, as many as nInstances / (nThreads)^2.
      // This way we can mitigate the slowdown from straggler threads.
      final int drainSize = Math.min(instances.size() / numTrainerThreads / numTrainerThreads, 1);

      for (int threadIdx = 0; threadIdx < numTrainerThreads; threadIdx++) {
        final Future<NMFModel> future = executor.submit(() -> {
          final List<NMFData> drainedInstances = new ArrayList<>(drainSize);
          final NMFModel model = modelAccessor.getModel()
              .orElseThrow(() -> new RuntimeException("Model was not initialized properly"));

          int count = 0;
          while (true) {
            final int numDrained = instances.drainTo(drainedInstances, drainSize);
            if (numDrained == 0) {
              break;
            }

            drainedInstances.forEach(instance -> updateModel(instance, model));
            drainedInstances.clear();
            count += numDrained;
          }
          latch.countDown();
          LOG.log(Level.INFO, "{0} has computed {1} instances",
              new Object[] {Thread.currentThread().getName(), count});
          return model;
        });
        futures.add(future);
      }
      latch.await();
      computeTracer.recordTime(numInstancesToProcess);
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Exception occurred.", e);
      throw new RuntimeException(e);
    }

    final List<NMFModel> newModels = ThreadUtils.retrieveResults(futures);
    final Map<Integer, Vector> gradients = aggregateGradient(newModels);

    // push gradients
    pushAndResetGradients(gradients);

    final double miniBatchElapsedTime = (System.currentTimeMillis() - miniBatchStartTime) / 1000.0D;
    final int epochIdx = miniBatchInfo.getEpochIdx();
    final int miniBatchIdx = miniBatchInfo.getMiniBatchIdx();

    final WorkerMetrics miniBatchMetric =
        buildMiniBatchMetric(epochIdx, miniBatchIdx,
            numInstancesToProcess, miniBatchElapsedTime);
    LOG.log(Level.INFO, "MiniBatchMetrics {0}", miniBatchMetric);
    sendMetrics(miniBatchMetric);
  }

  @Override
  public void onEpochFinished(final Collection<NMFData> epochData, final EpochInfo epochInfo) {
    LOG.log(Level.INFO, "Pull model to compute loss value");
    pullModels(getKeys(epochData));

    final NMFModel model = modelAccessor.getModel()
        .orElseThrow(() -> new RuntimeException("Model was not initialized properly"));

    LOG.log(Level.INFO, "Start computing loss value");
    final double loss = computeLoss(epochData, model);

    final int epochIdx = epochInfo.getEpochIdx();
    final int numMiniBatches = epochInfo.getNumMiniBatches();
    final int numEMBlocks = epochInfo.getNumEMBlocks();
    final double epochElapsedTime = (System.currentTimeMillis() - epochInfo.getEpochStartTime()) / 1000.0D;

    final WorkerMetrics epochMetric =
        buildEpochMetric(epochIdx, numMiniBatches, numEMBlocks,
            epochData.size(), loss, epochElapsedTime);

    LOG.log(Level.INFO, "EpochMetrics {0}", epochMetric);
    sendMetrics(epochMetric);

    if (decayRate != 1 && (epochIdx + 1) % decayPeriod == 0) {
      final double prevStepSize = stepSize;
      stepSize *= decayRate;
      LOG.log(Level.INFO, "{0} iterations have passed. Step size decays from {1} to {2}",
          new Object[]{decayPeriod, prevStepSize, stepSize});
    }
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
    pullModels(getKeys(workload));
    final NMFModel model = modelAccessor.getModel()
        .orElseThrow(() -> new RuntimeException("Model was not initialized properly"));

    final StringBuilder rsb = new StringBuilder();
    for (final Map.Entry<Integer, Vector> entry : model.getRMatrix().entrySet()) {
      rsb.append(String.format("R(*, %d):", entry.getKey()));
      for (final VectorEntry valueEntry : entry.getValue()) {
        rsb.append(' ');
        rsb.append(valueEntry.value());
      }
      rsb.append('\n');
    }
    LOG.log(Level.INFO, rsb.toString());
  }

  /**
   * Pull up-to-date model parameters from server, which become accessible via {@link ModelAccessor#getModel()}.
   * @param keys Column indices with which server stores the model parameters.
   */
  private void pullModels(final List<Integer> keys) {
    pullTracer.startTimer();
    final Map<Integer, Vector> rMatrix = new HashMap<>(keys.size());
    final List<Vector> vectors = parameterWorker.pull(keys);
    for (int i = 0; i < keys.size(); ++i) {
      rMatrix.put(keys.get(i), vectors.get(i));
    }

    modelAccessor.resetModel(new NMFModel(rMatrix));
    pullTracer.recordTime(keys.size());
  }

  /**
   * Processes one training data instance and update the intermediate model.
   * @param datum training data instance
   */
  private void updateModel(final NMFData datum, final NMFModel model) {
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
      final Vector rVec = model.getRMatrix().get(colIdx); // R_{*, j} : j-th column of R
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

      // accumulate R matrix's gradient
      accumulateRMatrixGradient(colIdx, rGrad, model);
    }

    // update L matrix
    modelGenerator.getValidVector(lVec.axpy(-stepSize, lGradSum));
  }

  /**
   * Aggregate the model computed by multiple threads, to get the gradients to push.
   * gradient[j] = sum(gradient_t[j]) where j is the column index of the gradient matrix.
   * @param newModels list of results (model parameters) computed by trainer threads
   * @return the gradient matrix
   */
  private Map<Integer, Vector> aggregateGradient(final List<NMFModel> newModels) {
    final Map<Integer, Vector> aggregated = new HashMap<>();
    newModels.forEach(nmfModel -> {
      final Map<Integer, Vector> gradient = nmfModel.getRGradient();
      gradient.forEach((k, v) -> {
        if (aggregated.containsKey(k)) {
          aggregated.get(k).addi(v);
        } else {
          aggregated.put(k, v);
        }
      });
    });
    return aggregated;
  }

  /**
   * Push the gradients to parameter server.
   * @param gradients vectors indexed by column indices each of which is gradient of a column of R matrix.
   */
  private void pushAndResetGradients(final Map<Integer, Vector> gradients) {
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
   * Compute the loss value using the current models and given data instances.
   * May take long, so do not call frequently.
   * @param instances The training data instances to evaluate training loss.
   * @return the loss value, computed by the sum of the errors.
   */
  private double computeLoss(final Collection<NMFData> instances, final NMFModel model) {
    final Map<Integer, Vector> rMatrix = model.getRMatrix();

    double loss = 0.0;
    for (final NMFData datum : instances) {
      final Vector lVec = datum.getVector(); // L_{i, *} : i-th row of L
      for (final Pair<Integer, Double> column : datum.getColumns()) { // a pair of column index and value
        final int colIdx = column.getFirst();
        final Vector rVec = rMatrix.get(colIdx); // R_{*, j} : j-th column of R
        final double error = lVec.dot(rVec) - column.getSecond(); // e = L_{i, *} * R_{*, j} - D_{i, j}
        loss += error * error;
      }
    }
    return loss;
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

  /**
   * Accumulates a new gradient into the R Matrix's gradient.
   * @param colIdx index of the column that the gradient is associated with
   * @param newGrad new gradient vector to accumulate
   * @param model current model parameters that contain R Matrix and its gradient
   */
  private void accumulateRMatrixGradient(final int colIdx, final Vector newGrad, final NMFModel model) {
    final Map<Integer, Vector> rMatrix = model.getRMatrix();
    final Map<Integer, Vector> gradients = model.getRGradient();

    final Vector grad = gradients.get(colIdx);
    if (grad == null) {
      // l2 regularization term. 2 * lambda * R_{*, j}
      if (lambda != 0.0D) {
        newGrad.axpy(2.0D * lambda, rMatrix.get(colIdx));
      }
      gradients.put(colIdx, newGrad);
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

  private WorkerMetrics buildMiniBatchMetric(final int iteration, final int miniBatchIdx,
                                             final int numProcessedDataItemCount, final double elapsedTime) {
    final Map<CharSequence, Double> appMetricMap = new HashMap<>();
    appMetricMap.put(NMFParameters.MetricKeys.DVT, numProcessedDataItemCount / elapsedTime);

    return WorkerMetrics.newBuilder()
        .setMetrics(Metrics.newBuilder()
            .setData(appMetricMap)
            .build())
        .setEpochIdx(iteration)
        .setMiniBatchSize(miniBatchSize)
        .setMiniBatchIdx(miniBatchIdx)
        .setProcessedDataItemCount(numProcessedDataItemCount)
        .setTotalTime(elapsedTime)
        .setTotalCompTime(computeTracer.totalElapsedTime())
        .setTotalPullTime(pullTracer.totalElapsedTime())
        .setAvgPullTime(pullTracer.avgTimePerElem())
        .setTotalPushTime(pushTracer.totalElapsedTime())
        .setAvgPushTime(pushTracer.avgTimePerElem())
        .setParameterWorkerMetrics(parameterWorker.buildParameterWorkerMetrics())
        .build();
  }

  private WorkerMetrics buildEpochMetric(final int iteration, final int numMiniBatchForEpoch,
                                         final int numDataBlocks, final int numProcessedDataItemCount,
                                         final double loss, final double elapsedTime) {
    final Map<CharSequence, Double> appMetricMap = new HashMap<>();
    appMetricMap.put(NMFParameters.MetricKeys.LOSS_SUM, loss);
    parameterWorker.buildParameterWorkerMetrics(); // clear ParameterWorker metrics

    return WorkerMetrics.newBuilder()
        .setMetrics(Metrics.newBuilder()
            .setData(appMetricMap)
            .build())
        .setEpochIdx(iteration)
        .setMiniBatchSize(miniBatchSize)
        .setNumMiniBatchForEpoch(numMiniBatchForEpoch)
        .setNumDataBlocks(numDataBlocks)
        .setProcessedDataItemCount(numProcessedDataItemCount)
        .setTotalTime(elapsedTime)
        .build();
  }
}
