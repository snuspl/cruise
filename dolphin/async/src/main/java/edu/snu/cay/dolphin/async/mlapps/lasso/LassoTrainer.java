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
package edu.snu.cay.dolphin.async.mlapps.lasso;

import edu.snu.cay.common.math.linalg.Matrix;
import edu.snu.cay.common.math.linalg.MatrixFactory;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.common.metric.MetricsMsgSender;
import edu.snu.cay.common.metric.avro.Metrics;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.EpochInfo;
import edu.snu.cay.dolphin.async.MiniBatchInfo;
import edu.snu.cay.dolphin.async.Trainer;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.dolphin.async.metric.Tracer;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.dolphin.async.mlapps.lasso.LassoParameters.*;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link Trainer} class for the LassoREEF application.
 * Based on lasso regression via stochastic coordinate descent, proposed in
 * S. Shalev-Shwartz and A. Tewari, Stochastic Methods for l1-regularized Loss Minimization, 2011.
 *
 * For each mini-batch, the trainer pulls the whole model from the server,
 * and then update all the model values.
 * The trainer computes and pushes the optimal model value for all the dimensions to
 * minimize the objective function - square loss with l1 regularization.
 */
final class LassoTrainer implements Trainer<LassoData> {
  private static final Logger LOG = Logger.getLogger(LassoTrainer.class.getName());

  /**
   * Period(epochs) to print model parameters to check whether the training is working or not.
   */
  private static final int PRINT_MODEL_PERIOD = 50;

  /**
   * Threshold for a number to be regarded as zero.
   */
  private static final double ZERO_THRESHOLD = 1e-9;

  private final int numFeatures;
  private final double lambda;
  private double stepSize;
  private final double decayRate;
  private final int decayPeriod;

  private final int miniBatchSize;

  private Vector oldModel;
  private Vector newModel;

  private final VectorFactory vectorFactory;
  private final MatrixFactory matrixFactory;

  /**
   * ParameterWorker object for interacting with the parameter server.
   */
  private final ParameterWorker<Integer, Vector, Vector> parameterWorker;
  
  /**
   * A list from 0 to {@code numPartitions} that will be used during {@link #pullModels()} and {@link #pushGradients()}.
   */
  private List<Integer> modelPartitionIndices;
  
  /**
   * Number of model partitions.
   */
  private final int numPartitions;
  
  /**
   * Number of features of each model partition.
   */
  private final int numFeaturesPerPartition;
  
  /**
   * To collect metric data.
   */
  private final MetricsMsgSender<WorkerMetrics> metricsMsgSender;
  private final Tracer pushTracer;
  private final Tracer pullTracer;
  private final Tracer computeTracer;

  @Inject
  private LassoTrainer(final ParameterWorker<Integer, Vector, Vector> parameterWorker,
                       @Parameter(Lambda.class) final double lambda,
                       @Parameter(NumFeatures.class) final int numFeatures,
                       @Parameter(StepSize.class) final double stepSize,
                       @Parameter(DecayRate.class) final double decayRate,
                       @Parameter(DecayPeriod.class) final int decayPeriod,
                       @Parameter(NumFeaturesPerPartition.class) final int numFeaturesPerPartition,
                       @Parameter(Parameters.MiniBatchSize.class) final int miniBatchSize,
                       final VectorFactory vectorFactory,
                       final MatrixFactory matrixFactory,
                       final MetricsMsgSender<WorkerMetrics> metricsMsgSender) {
    this.parameterWorker = parameterWorker;
    this.numFeatures = numFeatures;
    this.lambda = lambda;
    this.stepSize = stepSize;
    this.decayRate = decayRate;
    if (decayRate <= 0.0 || decayRate > 1.0) {
      throw new IllegalArgumentException("decay_rate must be larger than 0 and less than or equal to 1");
    }
    this.decayPeriod = decayPeriod;
    if (decayPeriod <= 0) {
      throw new IllegalArgumentException("decay_period must be a positive value");
    }
    this.miniBatchSize = miniBatchSize;
    this.vectorFactory = vectorFactory;
    this.matrixFactory = matrixFactory;
    this.oldModel = vectorFactory.createDenseZeros(numFeatures);
    if (numFeatures % numFeaturesPerPartition != 0) {
      throw new IllegalArgumentException("Uneven model partitions");
    }
    this.numFeaturesPerPartition = numFeaturesPerPartition;
    this.numPartitions = numFeatures / numFeaturesPerPartition;
    this.modelPartitionIndices = new ArrayList<>(numPartitions);
    for (int partitionIdx = 0; partitionIdx < numPartitions; partitionIdx++) {
      modelPartitionIndices.add(partitionIdx);
    }

    this.metricsMsgSender = metricsMsgSender;
    this.pullTracer = new Tracer();
    this.pushTracer = new Tracer();
    this.computeTracer = new Tracer();
  }

  @Override
  public void initGlobalSettings() {
  }

  /**
   * {@inheritDoc} <br>
   * 1) Pull model from server. <br>
   * 2) Compute the optimal value, dot(x_i, y - Sigma_{j != i} x_j * model(j)) / dot(x_i, x_i) for each dimension
   *    (in cyclic).
   *    When computing the optimal value, precalculate sigma_{all j} x_j * model(j) and calculate
   *    Sigma_{j != i} x_j * model(j) fast by just subtracting x_i * model(i)
   * 3) Push value to server.
   */
  @Override
  public void runMiniBatch(final Collection<LassoData> miniBatchData, final MiniBatchInfo miniBatchInfo) {
    resetTracer();

    final int epochIdx = miniBatchInfo.getEpochIdx();
    final int miniBatchIdx = miniBatchInfo.getMiniBatchIdx();
    final int numInstancesToProcess = miniBatchData.size();
    final long miniBatchStartTime = System.currentTimeMillis();

    pullModels();

    computeTracer.startTimer();
    // After get feature vectors from each instances, make it concatenate them into matrix for the faster calculation.
    // Pre-calculate sigma_{all j} x_j * model(j) and assign the value into 'preCalculate' vector.
    final Pair<Matrix, Vector> featureMatrixAndValues = convertToFeaturesAndValues(miniBatchData);
    final Matrix featureMatrix = featureMatrixAndValues.getLeft();
    final Vector yValues = featureMatrixAndValues.getRight();

    final Vector preCalculate = featureMatrix.mmul(newModel);

    final Vector columnVector = vectorFactory.createDenseZeros(numInstancesToProcess);
    // For each dimension, compute the optimal value.
    for (int feature = 0; feature < numFeatures; feature++) {
      if (closeToZero(newModel.get(feature))) {
        continue;
      }
      for (int row = 0; row < numInstancesToProcess; row++) {
        columnVector.set(row, featureMatrix.get(row, feature));
      }
      final double columnNorm = columnVector.dot(columnVector);
      if (closeToZero(columnNorm)) {
        continue;
      }
      preCalculate.subi(columnVector.scale(newModel.get(feature)));
      newModel.set(feature, sthresh((columnVector.dot(yValues.sub(preCalculate))) / columnNorm, lambda, columnNorm));
      preCalculate.addi(columnVector.scale(newModel.get(feature)));
    }
    computeTracer.recordTime(numInstancesToProcess);

    pushGradients();

    final double miniBatchElapsedTime = (System.currentTimeMillis() - miniBatchStartTime) / 1000.0D;

    final WorkerMetrics miniBatchMetric =
        buildMiniBatchMetric(epochIdx, miniBatchIdx, numInstancesToProcess, miniBatchElapsedTime);
    LOG.log(Level.INFO, "MiniBatchMetrics {0}", miniBatchMetric);
    sendMetrics(miniBatchMetric);
  }

  @Override
  public void onEpochFinished(final Collection<LassoData> epochData, final EpochInfo epochInfo) {
    final int epochIdx = epochInfo.getEpochIdx();

    // Calculate the loss value.
    pullModels();

    final double sampleLossSum = computeLoss(epochData);
    LOG.log(Level.INFO, "Loss value: {0}", sampleLossSum);
    if ((epochIdx + 1) % PRINT_MODEL_PERIOD == 0) {
      for (int i = 0; i < numFeatures; i++) {
        LOG.log(Level.INFO, "model : {0}", newModel.get(i));
      }
    }

    final int numMiniBatches = epochInfo.getNumMiniBatches();
    final int numEMBlocks = epochInfo.getNumEMBlocks();
    final double epochElapsedTime = (System.currentTimeMillis() - epochInfo.getEpochStartTime()) / 1000.0D;

    final WorkerMetrics epochMetric =
        buildEpochMetric(epochIdx, numMiniBatches, numEMBlocks, epochData.size(), sampleLossSum, epochElapsedTime);

    LOG.log(Level.INFO, "EpochMetrics {0}", epochMetric);
    sendMetrics(epochMetric);

    if (decayRate != 1 && (epochIdx + 1) % decayPeriod == 0) {
      final double prevStepSize = stepSize;
      stepSize *= decayRate;
      LOG.log(Level.INFO, "{0} epochs have passed. Step size decays from {1} to {2}",
          new Object[]{decayPeriod, prevStepSize, stepSize});
    }
  }

  /**
   * Convert the training data examples into a form for more efficient computation.
   * @param instances training data examples
   * @return the pair of feature matrix and vector composed of y values.
   */
  private Pair<Matrix, Vector> convertToFeaturesAndValues(final Collection<LassoData> instances) {
    final List<Vector> features = new LinkedList<>();
    final Vector values = vectorFactory.createDenseZeros(instances.size());
    int instanceIdx = 0;
    for (final LassoData instance : instances) {
      features.add(instance.getFeature());
      values.set(instanceIdx++, instance.getValue());
    }
    return Pair.of(matrixFactory.horzcatVecSparse(features).transpose(), values);
  }

  @Override
  public void cleanup() {
  }

  /**
   * Pull up-to-date model parameters from server.
   */
  private void pullModels() {
    pullTracer.startTimer();
    final List<Vector> partialModels = parameterWorker.pull(modelPartitionIndices);
    pullTracer.recordTime(numPartitions);
    computeTracer.startTimer();
    oldModel = vectorFactory.concatDense(partialModels);
    newModel = oldModel.copy();
    computeTracer.recordTime(0);
  }

  /**
   * Push the gradients to parameter server.
   */
  private void pushGradients() {
    computeTracer.startTimer();
    final Vector gradient = newModel.sub(oldModel);
    computeTracer.recordTime(0);
    pushTracer.startTimer();
    for (int partitionIndex = 0; partitionIndex < numPartitions; ++partitionIndex) {
      final int partitionStart = partitionIndex * numFeaturesPerPartition;
      final int partitionEnd = (partitionIndex + 1) * numFeaturesPerPartition;
      parameterWorker.push(partitionIndex, vectorFactory.createDenseZeros(numFeaturesPerPartition)
          .axpy(stepSize, gradient.slice(partitionStart, partitionEnd)));
    }
    pushTracer.recordTime(numPartitions);
  }

  /**
   * Soft thresholding function, widely used with l1 regularization.
   */
  private static double sthresh(final double x, final double lambda, final double columnNorm) {
    if (Math.abs(x) <= lambda / columnNorm) {
      return 0;
    } else if (x >= 0) {
      return x - lambda / columnNorm;
    } else {
      return x + lambda / columnNorm;
    }
  }

  /**
   * Predict the y value for the feature value.
   */
  private double predict(final Vector feature) {
    return newModel.dot(feature);
  }

  /**
   * Compute the loss value for the data.
   */
  private double computeLoss(final Collection<LassoData> data) {
    double squaredErrorSum = 0;

    for (final LassoData entry : data) {
      final Vector feature = entry.getFeature();
      final double value = entry.getValue();
      final double prediction = predict(feature);
      squaredErrorSum += (value - prediction) * (value - prediction);
    }

    return squaredErrorSum;
  }

  private void resetTracer() {
    pullTracer.resetTrace();
    pushTracer.resetTrace();
    computeTracer.resetTrace();
  }

  private void sendMetrics(final WorkerMetrics workerMetrics) {
    LOG.log(Level.FINE, "Sending WorkerMetrics {0}", workerMetrics);

    metricsMsgSender.send(workerMetrics);
  }

  private WorkerMetrics buildMiniBatchMetric(final int epochIdx, final int miniBatchIdx,
                                             final int numProcessedDataItemCount, final double elapsedTime) {
    final Map<CharSequence, Double> appMetricMap = new HashMap<>();
    appMetricMap.put(MetricKeys.DVT, numProcessedDataItemCount / elapsedTime);

    return WorkerMetrics.newBuilder()
        .setMetrics(Metrics.newBuilder()
            .setData(appMetricMap)
            .build())
        .setEpochIdx(epochIdx)
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

  private WorkerMetrics buildEpochMetric(final int epochIdx, final int numMiniBatchForEpoch,
                                         final int numDataBlocks, final int numProcessedDataItemCount,
                                         final double loss, final double elapsedTime) {
    final Map<CharSequence, Double> appMetricMap = new HashMap<>();
    appMetricMap.put(MetricKeys.SAMPLE_LOSS_SUM, loss);
    parameterWorker.buildParameterWorkerMetrics(); // clear ParameterWorker metrics

    return WorkerMetrics.newBuilder()
        .setMetrics(Metrics.newBuilder()
            .setData(appMetricMap)
            .build())
        .setEpochIdx(epochIdx)
        .setMiniBatchSize(miniBatchSize)
        .setNumMiniBatchForEpoch(numMiniBatchForEpoch)
        .setNumDataBlocks(numDataBlocks)
        .setProcessedDataItemCount(numProcessedDataItemCount)
        .setTotalTime(elapsedTime)
        .build();
  }

  /**
   * @return {@code true} if the value is close to 0.
   */
  private boolean closeToZero(final double value) {
    return Math.abs(value) < ZERO_THRESHOLD;
  }
}
