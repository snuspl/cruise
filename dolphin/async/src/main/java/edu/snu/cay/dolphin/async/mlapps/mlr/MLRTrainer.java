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
package edu.snu.cay.dolphin.async.mlapps.mlr;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.common.metric.MetricsMsgSender;
import edu.snu.cay.common.metric.avro.Metrics;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.MiniBatchParameterWorker;
import edu.snu.cay.dolphin.async.TrainingDataSplitter;
import edu.snu.cay.dolphin.async.Trainer;
import edu.snu.cay.dolphin.async.metric.Tracer;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.utils.Tuple3;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.mlapps.mlr.MLRParameters.*;

/**
 * {@link Trainer} class for the MLRREEF application.
 * Uses {@code numClasses} model vectors to determine which class each data instance belongs to.
 * The model vector that outputs the highest dot product value is declared as that data instance's prediction.
 */
final class MLRTrainer implements Trainer {
  private static final Logger LOG = Logger.getLogger(MLRTrainer.class.getName());

  /**
   * MiniBatchParameterWorker object used to interact with ParameterWorker.
   */
  private final MiniBatchParameterWorker<Integer, Vector, Vector> miniBatchParameterWorker;

  /**
   * Number of possible classes for a data instance.
   */
  private final int numClasses;

  /**
   * Number of features of each model partition.
   */
  private final int numFeaturesPerPartition;

  /**
   * Number of model partitions for each class.
   */
  private final int numPartitionsPerClass;

  /**
   * Number of batches per epoch.
   */
  private final int numMiniBatchPerEpoch;

  /**
   * Size of each step taken during gradient descent.
   */
  private double stepSize;

  /**
   * L2 regularization constant.
   */
  private final double lambda;

  /**
   * Object for creating {@link Vector} instances.
   */
  private final VectorFactory vectorFactory;

  /**
   * A list from 0 to {@code numClasses * numPartitionsPerClass} that will be used during {@link #pullModels()}.
   */
  private List<Integer> classPartitionIndices;

  /**
   * The step size drops by this rate.
   */
  private final double decayRate;

  /**
   * The step size drops after {@code decayPeriod} iterations pass.
   */
  private final int decayPeriod;

  /**
   * Number of instances to compute training loss with.
   */
  private final int trainErrorDatasetSize;

  private final TrainingDataSplitter<Long> trainingDataSplitter;

  // TODO #487: Metric collecting should be done by the system, not manually by the user code.
  private final MetricsMsgSender<WorkerMetrics> metricsMsgSender;
  private final ParameterWorker parameterWorker;
  private final MemoryStore<Long> memoryStore;

  private final Tracer pushTracer;
  private final Tracer pullTracer;
  private final Tracer computeTracer;

  private long epochBegin;

  @Inject
  private MLRTrainer(final MiniBatchParameterWorker<Integer, Vector, Vector> miniBatchParameterWorker,
                     @Parameter(NumClasses.class) final int numClasses,
                     @Parameter(NumFeatures.class) final int numFeatures,
                     @Parameter(NumFeaturesPerPartition.class) final int numFeaturesPerPartition,
                     @Parameter(InitialStepSize.class) final double initStepSize,
                     @Parameter(Lambda.class) final double lambda,
                     @Parameter(DecayRate.class) final double decayRate,
                     @Parameter(DecayPeriod.class) final int decayPeriod,
                     @Parameter(TrainErrorDatasetSize.class) final int trainErrorDatasetSize,
                     @Parameter(Parameters.MiniBatches.class) final int numMiniBatchPerEpoch,
                     final TrainingDataSplitter<Long> trainingDataSplitter,
                     final MetricsMsgSender<WorkerMetrics> metricsMsgSender,
                     final ParameterWorker parameterWorker,
                     final MemoryStore<Long> memoryStore,
                     final VectorFactory vectorFactory) {
    this.miniBatchParameterWorker = miniBatchParameterWorker;
    this.numClasses = numClasses;
    this.numFeaturesPerPartition = numFeaturesPerPartition;
    if (numFeatures % numFeaturesPerPartition != 0) {
      throw new RuntimeException("Uneven model partitions");
    }
    this.numPartitionsPerClass = numFeatures / numFeaturesPerPartition;
    this.numMiniBatchPerEpoch = numMiniBatchPerEpoch;
    this.stepSize = initStepSize;
    this.lambda = lambda;
    this.vectorFactory = vectorFactory;
    this.decayRate = decayRate;
    this.decayPeriod = decayPeriod;
    this.trainErrorDatasetSize = trainErrorDatasetSize;
    this.metricsMsgSender = metricsMsgSender;
    this.parameterWorker = parameterWorker;
    this.memoryStore = memoryStore;
    this.trainingDataSplitter = trainingDataSplitter;
    this.pushTracer = new Tracer();
    this.pullTracer = new Tracer();
    this.computeTracer = new Tracer();
  }

  /**
   * Parse the input dataset, initialize a few data structures, and wait for other workers.
   */
  @Override
  public void initialize() {
    classPartitionIndices = new ArrayList<>(numClasses * numPartitionsPerClass);
    for (int classIndex = 0; classIndex < numClasses; ++classIndex) {
      for (int partitionIndex = 0; partitionIndex < numPartitionsPerClass; ++partitionIndex) {
        // 0 ~ (numPartitionsPerClass - 1) is for class 0
        // numPartitionsPerClass ~ (2 * numPartitionsPerClass - 1) is for class 1
        // and so on
        classPartitionIndices.add(classIndex * numPartitionsPerClass + partitionIndex);
      }
    }

    final int dataValuesSize = memoryStore.getAll().size();
    LOG.log(Level.INFO, "Step size = {0}", stepSize);
    LOG.log(Level.INFO, "Number of batches per iteration = {0}", numMiniBatchPerEpoch);
    LOG.log(Level.INFO, "Total number of keys = {0}", numClasses * numPartitionsPerClass);
    LOG.log(Level.INFO, "Total number of training data items = {0}", dataValuesSize);
    if (dataValuesSize < trainErrorDatasetSize) {
      LOG.log(Level.WARNING, "Number of samples is less than trainErrorDatasetSize = {0}", trainErrorDatasetSize);
    }
  }

  @Override
  public void initEpochVariables(final int epoch) {
    epochBegin = System.currentTimeMillis();
    resetTracers();
  }

  @Override
  public void wrapUpEpochVariables(final int epoch) {
    if (epoch % decayPeriod == 0) {
      final double prevStepSize = stepSize;
      stepSize *= decayRate;
      LOG.log(Level.INFO, "{0} epochs have passed. Step size decays from {1} to {2}",
          new Object[]{decayPeriod, prevStepSize, stepSize});
    }

    // Record the number of EM data blocks at the beginning of this iteration
    // to filter out stale metrics for optimization
    final int numEMBlocks = memoryStore.getNumBlocks();
    final Map<Long, Pair<Vector, Integer>> workloadMap = memoryStore.getAll();
    final List<Pair<Vector, Integer>> workload = new ArrayList<>(workloadMap.values());

    final double elapsedTime = (System.currentTimeMillis() - epochBegin) / 1000.0D;
    final Tuple3<Double, Double, Float> lossRegLossAccuracy = computeLoss(trainErrorDatasetSize, workload);
    final Metrics appMetrics = buildAppMetrics(lossRegLossAccuracy.getFirst(),
        lossRegLossAccuracy.getSecond(), (double) lossRegLossAccuracy.getThird(), elapsedTime, workload.size());
    final WorkerMetrics workerMetrics =
        buildMetricsMsg(epoch, appMetrics, numEMBlocks, workload.size(), elapsedTime);

    LOG.log(Level.INFO, "WorkerMetrics {0}", workerMetrics);
    sendMetrics(workerMetrics);
  }

  @Override
  public void run() {

    final Map<Long, Pair<Vector, Integer>> workloadMap = trainingDataSplitter.getNextTrainingDataSplit();
    final List<Pair<Vector, Integer>> workload = new ArrayList<>(workloadMap.values());

    int numInstances = 0;
    final Vector[] models = pullModels();
    computeTracer.startTimer();
    for (final Pair<Vector, Integer> entry : workload) {

      final Vector features = entry.getFirst();
      final int label = entry.getSecond();

      // compute h(x, w) = softmax(x dot w)
      final Vector predictions = predict(features);

      // error = h(x, w) - y, where y_j = 1 (if positive for class j) or 0 (otherwise)
      // instead of allocating a new vector for the error,
      // we use the same object for convenience
      predictions.set(label, predictions.get(label) - 1);

      // gradient_j = -stepSize * error_j * x
      for (int j = 0; j < numClasses; ++j) {
        final Vector newModel = models[j];
        if (lambda != 0) {
          newModel.axpy(-predictions.get(j) * stepSize, features);
          newModel.axpy(-stepSize * lambda, newModel);
        } else {
          newModel.axpy(-predictions.get(j) * stepSize, features);
        }
        computeTracer.recordTime(0);
        pushModel(newModel);
        computeTracer.startTimer();
      }
      ++numInstances;
    }

    computeTracer.recordTime(numInstances);
  }

  /**
   * Pull models one last time and perform validation.
   */
  @Override
  public void cleanup() {
    final Map<Long, Pair<Vector, Integer>> workloadMap = memoryStore.getAll();
    final List<Pair<Vector, Integer>> data = new ArrayList<>(workloadMap.values());
    final int entireDatasetSize = data.size();

    // Compute loss with the entire dataset.
    final Tuple3<Double, Double, Float> lossRegLossAccuracy = computeLoss(entireDatasetSize, data);
    final Metrics appMetrics =
        buildAppMetrics(lossRegLossAccuracy.getFirst(), lossRegLossAccuracy.getSecond(), lossRegLossAccuracy.getThird(),
            0.0, entireDatasetSize);
    LOG.log(Level.INFO, "[Cleanup] AppMetrics {0}", appMetrics);
  }

  private Vector[] pullModels() {
    pullTracer.startTimer();
    final List<Vector> partitions = miniBatchParameterWorker.pull(classPartitionIndices);
    pullTracer.recordTime(partitions.size());
    computeTracer.startTimer();
    final Vector[] models = new Vector[numClasses];
    for (int classIndex = 0; classIndex < numClasses; ++classIndex) {
      // 0 ~ (numPartitionsPerClass - 1) is for class 0
      // numPartitionsPerClass ~ (2 * numPartitionsPerClass - 1) is for class 1
      // and so on
      final List<Vector> partialModelsForThisClass =
          partitions.subList(classIndex * numPartitionsPerClass, (classIndex + 1) * numPartitionsPerClass);
      // concat partitions into one long vector
      models[classIndex] = vectorFactory.concatDense(partialModelsForThisClass);
    }
    computeTracer.recordTime(0);
    return models;
  }

  private void pushModel(final Vector model) {
    for (int classIndex = 0; classIndex < numClasses; classIndex++) {
      pushTracer.startTimer();
      for (int partitionIndex = 0; partitionIndex < numPartitionsPerClass; ++partitionIndex) {
        final int partitionStart = partitionIndex * numFeaturesPerPartition;
        final int partitionEnd = (partitionIndex + 1) * numFeaturesPerPartition;
        miniBatchParameterWorker.push(classIndex * numPartitionsPerClass + partitionIndex,
            model.slice(partitionStart, partitionEnd));
      }
      pushTracer.recordTime(numPartitionsPerClass);
    }
  }

  /**
   * Compute the loss value using the current models and all data instances.
   * May take long, so do not call frequently.
   */
  private Tuple3<Double, Double, Float> computeLoss(final int datasetSize,
                                                    final List<Pair<Vector, Integer>> data) {
    double loss = 0;
    int numInstances = 0;
    int correctPredictions = 0;

    final int numDataToCompute = Math.min(datasetSize, data.size());
    for (final Pair<Vector, Integer> entry : data.subList(0, numDataToCompute)) {
      final Vector features = entry.getFirst();
      final int label = entry.getSecond();
      final Vector predictions = predict(features);
      final int prediction = max(predictions).getFirst();

      if (label == prediction) {
        ++correctPredictions;
      }

      for (int classIndex = 0; classIndex < numClasses; ++classIndex) {
        if (classIndex == label) {
          loss += -Math.log(predictions.get(classIndex));
        } else {
          loss += -Math.log(1 - predictions.get(classIndex));
        }
      }

      ++numInstances;
    }
    loss /= numInstances;

    double regLoss = 0;
    if (lambda != 0) {
      final Vector[] models = pullModels();
      // skip this part entirely if lambda is zero, to avoid regularization operation overheads
      for (int classIndex = 0; classIndex < numClasses; ++classIndex) {
        final Vector model = models[classIndex];
        double l2norm = 0;
        for (int vectorIndex = 0; vectorIndex < model.length(); ++vectorIndex) {
          l2norm += model.get(vectorIndex) * model.get(vectorIndex);
        }
        regLoss += l2norm * lambda / 2;
      }
    }
    regLoss /= numClasses;
    return new Tuple3<>(loss, regLoss, (float) correctPredictions / numInstances);
  }

  /**
   * Compute the probability vector of the given data instance, represented by {@code features}.
   */
  private Vector predict(final Vector features) {
    final double[] predict = new double[numClasses];
    final Vector[] models = pullModels();
    for (int classIndex = 0; classIndex < numClasses; ++classIndex) {
      final Vector model = models[classIndex];
      predict[classIndex] = model.dot(features);
    }
    return softmax(vectorFactory.createDense(predict));
  }

  private static Vector softmax(final Vector vector) {
    // prevent overflow during exponential operations
    // https://lingpipe-blog.com/2009/06/25/log-sum-of-exponentials/
    final double logSumExp = logSumExp(vector);
    for (int index = 0; index < vector.length(); ++index) {
      vector.set(index, Math.max(Math.min(1 - 1e-12, Math.exp(vector.get(index) - logSumExp)), 1e-12));
    }
    return vector;
  }

  /**
   * Returns {@code log(sum_i(exp(vector.get(i)))}, while avoiding overflow.
   */
  private static double logSumExp(final Vector vector) {
    final double max = max(vector).getSecond();
    double sumExp = 0;
    for (int index = 0; index < vector.length(); ++index) {
      sumExp += Math.exp(vector.get(index) - max);
    }
    return max + Math.log(sumExp);
  }

  /**
   * Find the largest value in {@code vector} and return its index and the value itself together.
   */
  private static Pair<Integer, Double> max(final Vector vector) {
    double maxValue = vector.get(0);
    int maxIndex = 0;
    for (int index = 1; index < vector.length(); ++index) {
      final double value = vector.get(index);
      if (value > maxValue) {
        maxValue = value;
        maxIndex = index;
      }
    }
    return new Pair<>(maxIndex, maxValue);
  }

  private void resetTracers() {
    pushTracer.resetTrace();
    pullTracer.resetTrace();
    computeTracer.resetTrace();
  }

  private void sendMetrics(final WorkerMetrics workerMetrics) {
    LOG.log(Level.FINE, "Sending WorkerMetrics {0}", new Object[]{workerMetrics});

    metricsMsgSender.send(workerMetrics);
  }

  private WorkerMetrics buildMetricsMsg(final int iteration, final Metrics appMetrics, final int numDataBlocks,
                                        final int numProcessedDataItemCount, final double elapsedTime) {
    final WorkerMetrics workerMetrics = WorkerMetrics.newBuilder()
        .setMetrics(appMetrics)
        .setItrIdx(iteration)
        .setNumMiniBatchPerItr(numMiniBatchPerEpoch)
        .setNumDataBlocks(numDataBlocks)
        .setProcessedDataItemCount(numProcessedDataItemCount)
        .setTotalTime(elapsedTime)
        .setTotalCompTime(computeTracer.totalElapsedTime())
        .setTotalPullTime(pullTracer.totalElapsedTime())
        .setAvgPullTime(pullTracer.avgTimePerElem())
        .setTotalPushTime(pushTracer.totalElapsedTime())
        .setAvgPushTime(pushTracer.avgTimePerElem())
        .setParameterWorkerMetrics(parameterWorker.buildParameterWorkerMetrics())
        .build();

    return workerMetrics;
  }

  private Metrics buildAppMetrics(final double sampleLoss, final double regLoss, final double accuracy,
                                  final double elapsedTime, final int numInstances) {
    final Map<CharSequence, Double> appMetricMap = new HashMap<>();
    appMetricMap.put(MetricKeys.SAMPLE_LOSS_AVG, sampleLoss);
    appMetricMap.put(MetricKeys.REG_LOSS_AVG, regLoss);
    appMetricMap.put(MetricKeys.ACCURACY, accuracy);
    appMetricMap.put(MetricKeys.DVT, numInstances / elapsedTime);

    return Metrics.newBuilder()
        .setData(appMetricMap)
        .build();
  }
}
