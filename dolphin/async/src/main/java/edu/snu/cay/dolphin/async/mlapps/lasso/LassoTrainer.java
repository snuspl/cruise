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
 * For each iteration, the trainer pulls the whole model from the server,
 * and then update all the model values.
 * The trainer computes and pushes the optimal model value for all the dimensions to
 * minimize the objective function - square loss with l1 regularization.
 */
final class LassoTrainer implements Trainer<LassoData> {
  private static final Logger LOG = Logger.getLogger(LassoTrainer.class.getName());

  /**
   * Period(iterations) to print model parameters to check whether the training is working or not.
   */
  private static final int PRINT_MODEL_PERIOD = 50;

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
  private final ParameterWorker<Integer, Double, Double> parameterWorker;

  /**
   * To collect metric data.
   */
  private final MetricsMsgSender<WorkerMetrics> metricsMsgSender;
  private final Tracer pushTracer;
  private final Tracer pullTracer;
  private final Tracer computeTracer;

  @Inject
  private LassoTrainer(final ParameterWorker<Integer, Double, Double> parameterWorker,
                       @Parameter(Lambda.class) final double lambda,
                       @Parameter(NumFeatures.class) final int numFeatures,
                       @Parameter(StepSize.class) final double stepSize,
                       @Parameter(DecayRate.class) final double decayRate,
                       @Parameter(DecayPeriod.class) final int decayPeriod,
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
    this.metricsMsgSender = metricsMsgSender;
    this.pullTracer = new Tracer();
    this.pushTracer = new Tracer();
    this.computeTracer = new Tracer();
  }

  @Override
  public void initialize() {
    oldModel = vectorFactory.createDenseZeros(numFeatures);
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
    // Pre-calculate sigma_{all j} x_j * model(j) and assign the value into pre-calculate vector.
    final Pair<Matrix, Vector> featureMatrixAndValues = convertToFeaturesAndValues(miniBatchData);
    final Matrix featureMatrix = featureMatrixAndValues.getLeft();
    final Vector yValues = featureMatrixAndValues.getRight();

    final Vector preCalculate = featureMatrix.mmul(newModel);

    // For each dimension, compute the optimal value.
    for (int i = 0; i < numFeatures; i++) {
      final Vector columnVector = featureMatrix.sliceColumn(i);
      final double columnNorm = columnVector.dot(columnVector);
      if (columnNorm == 0 || newModel.get(i) == 0) {
        continue;
      }
      preCalculate.subi(columnVector.scale(newModel.get(i)));
      newModel.set(i, sthresh((columnVector.dot(yValues.sub(preCalculate))) / columnNorm, lambda, columnNorm));
      preCalculate.addi(columnVector.scale(newModel.get(i)));
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
      LOG.log(Level.INFO, "{0} iterations have passed. Step size decays from {1} to {2}",
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
    return Pair.of(matrixFactory.horzcatVecDense(features).transpose(), values);
  }

  @Override
  public void cleanup() {
  }

  /**
   * Pull up-to-date model parameters from server.
   */
  private void pullModels() {
    pullTracer.startTimer();
    for (int modelIndex = 0; modelIndex < numFeatures; modelIndex++) {
      oldModel.set(modelIndex, parameterWorker.pull(modelIndex));
    }
    pullTracer.recordTime(numFeatures);
    newModel = oldModel.copy();
  }

  /**
   * Push the gradients to parameter server.
   */
  private void pushGradients() {
    computeTracer.startTimer();
    final Vector gradient = newModel.sub(oldModel);
    computeTracer.recordTime(0);
    pushTracer.startTimer();
    for (int modelIndex = 0; modelIndex < numFeatures; ++modelIndex) {
      parameterWorker.push(modelIndex, stepSize * gradient.get(modelIndex));
    }
    pushTracer.recordTime(numFeatures);
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

  private WorkerMetrics buildMiniBatchMetric(final int iteration, final int miniBatchIdx,
                                             final int numProcessedDataItemCount, final double elapsedTime) {
    final Map<CharSequence, Double> appMetricMap = new HashMap<>();
    appMetricMap.put(MetricKeys.DVT, numProcessedDataItemCount / elapsedTime);

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
    appMetricMap.put(MetricKeys.SAMPLE_LOSS_SUM, loss);
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
