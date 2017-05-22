/*
 * Copyright (C) 2017 Seoul National University
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
import edu.snu.cay.dolphin.async.*;
import edu.snu.cay.utils.ThreadUtils;
import edu.snu.cay.utils.Tuple3;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.mlapps.mlr.MLRParameters.*;

/**
 * {@link Trainer} class for the MLRREEF application.
 * Uses {@code numClasses} model vectors to determine which class each data instance belongs to.
 * The model vector that outputs the highest dot product value is declared as that data instance's prediction.
 */
final class MLRTrainer implements Trainer<MLRData> {
  private static final Logger LOG = Logger.getLogger(MLRTrainer.class.getName());

  private final ModelAccessor<Integer, Vector, Vector> modelAccessor;

  /**
   * Number of possible classes for a data instance.
   */
  private final int numClasses;
  
  /**
   * Number of features for a data instance.
   */
  private final int numFeatures;
  
  /**
   * Number of features of each model partition.
   */
  private final int numFeaturesPerPartition;

  /**
   * Number of model partitions for each class.
   */
  private final int numPartitionsPerClass;

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
   * Preserves the model parameters that are pulled from server, in order to compute gradients.
   */
  private final MLRModel model;

  /**
   * A list from 0 to {@code numClasses * numPartitionsPerClass} that will be used during {@link #pullModels()}.
   */
  private List<Integer> classPartitionIndices;

  /**
   * The step size drops by this rate.
   */
  private final double decayRate;

  /**
   * The step size drops after every {@code decayPeriod} epochs pass.
   */
  private final int decayPeriod;

  /**
   * Executes the Trainer threads.
   */
  private final ExecutorService executor;

  /**
   * Number of Trainer threads that train concurrently.
   */
  private final int numTrainerThreads;

  @Inject
  private MLRTrainer(final ModelAccessor<Integer, Vector, Vector> modelAccessor,
                     @Parameter(NumClasses.class) final int numClasses,
                     @Parameter(NumFeatures.class) final int numFeatures,
                     @Parameter(NumFeaturesPerPartition.class) final int numFeaturesPerPartition,
                     @Parameter(InitialStepSize.class) final double initStepSize,
                     @Parameter(Lambda.class) final double lambda,
                     @Parameter(DecayRate.class) final double decayRate,
                     @Parameter(DecayPeriod.class) final int decayPeriod,
                     @Parameter(DolphinParameters.MiniBatchSize.class) final int miniBatchSize,
                     @Parameter(DolphinParameters.NumTrainerThreads.class) final int numTrainerThreads,
                     final VectorFactory vectorFactory) {
    this.modelAccessor = modelAccessor;
    this.numClasses = numClasses;
    this.numFeatures = numFeatures;
    this.numFeaturesPerPartition = numFeaturesPerPartition;
    if (numFeatures % numFeaturesPerPartition != 0) {
      throw new RuntimeException("Uneven model partitions");
    }
    this.numPartitionsPerClass = numFeatures / numFeaturesPerPartition;
    this.stepSize = initStepSize;
    this.lambda = lambda;
    this.vectorFactory = vectorFactory;
    this.model = new MLRModel(new Vector[numClasses]);

    this.decayRate = decayRate;
    if (decayRate <= 0.0 || decayRate > 1.0) {
      throw new IllegalArgumentException("decay_rate must be larger than 0 and less than or equal to 1");
    }
    this.decayPeriod = decayPeriod;
    if (decayPeriod <= 0) {
      throw new IllegalArgumentException("decay_period must be a positive value");
    }

    this.numTrainerThreads = numTrainerThreads;
    this.executor = Executors.newFixedThreadPool(numTrainerThreads);

    this.classPartitionIndices = new ArrayList<>(numClasses * numPartitionsPerClass);
    for (int classIndex = 0; classIndex < numClasses; ++classIndex) {
      for (int partitionIndex = 0; partitionIndex < numPartitionsPerClass; ++partitionIndex) {
        // 0 ~ (numPartitionsPerClass - 1) is for class 0
        // numPartitionsPerClass ~ (2 * numPartitionsPerClass - 1) is for class 1
        // and so on
        classPartitionIndices.add(classIndex * numPartitionsPerClass + partitionIndex);
      }
    }

    LOG.log(Level.INFO, "Number of Trainer threads = {0}", numTrainerThreads);
    LOG.log(Level.INFO, "Step size = {0}", stepSize);
    LOG.log(Level.INFO, "Number of instances per mini-batch = {0}", miniBatchSize);
    LOG.log(Level.INFO, "Total number of keys = {0}", classPartitionIndices.size());
  }

  @Override
  public void initGlobalSettings() {
  }

  @Override
  public void runMiniBatch(final Collection<MLRData> miniBatchTrainingData) {
    // pull data when mini-batch is started
    pullModels();

    final CountDownLatch latch = new CountDownLatch(numTrainerThreads);

    final BlockingQueue<MLRData> instances = new ArrayBlockingQueue<>(miniBatchTrainingData.size());
    instances.addAll(miniBatchTrainingData);

    // collects the results (new models here) computed by multiple threads
    final List<Future<Vector[]>> futures = new ArrayList<>(numTrainerThreads);
    try {
      // Threads drain multiple instances from shared queue, as many as nInstances / (nThreads)^2.
      // This way we can mitigate the slowdown from straggler threads.
      final int drainSize = Math.max(instances.size() / numTrainerThreads / numTrainerThreads, 1);

      for (int threadIdx = 0; threadIdx < numTrainerThreads; threadIdx++) {
        final Future<Vector[]> future = executor.submit(() -> {
          final List<MLRData> drainedInstances = new ArrayList<>(drainSize);
          final Vector[] threadGradient = new Vector[numClasses];
          for (int classIdx = 0; classIdx < numClasses; classIdx++) {
            threadGradient[classIdx] = vectorFactory.createDenseZeros(numFeatures);
          }
          
          int count = 0;
          while (true) {
            final int numDrained = instances.drainTo(drainedInstances, drainSize);
            if (numDrained == 0) {
              break;
            }
            
            drainedInstances.forEach(instance -> updateGradient(instance, threadGradient));
            drainedInstances.clear();
            count += numDrained;
          }
          latch.countDown();
          LOG.log(Level.INFO, "{0} has computed {1} instances",
              new Object[] {Thread.currentThread().getName(), count});
          return threadGradient;
        });
        futures.add(future);
      }
      latch.await();
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Exception occurred.", e);
      throw new RuntimeException(e);
    }

    final List<Vector[]> threadGradients = ThreadUtils.retrieveResults(futures);
    final Vector[] gradients = aggregateGradient(threadGradients);

    // push gradients
    pushAndResetGradients(gradients);
  }

  @Override
  public EpochResult onEpochFinished(final Collection<MLRData> epochTrainingData,
                                     final Collection<MLRData> testData,
                                     final int epochIdx) {
    LOG.log(Level.INFO, "Pull model to compute loss value");
    pullModels();

    LOG.log(Level.INFO, "Start computing loss value");
    final Vector[] params = model.getParams();
    final Tuple3<Double, Double, Double> trainingLossRegLossAvgAccuracy = computeLoss(epochTrainingData, params);
    final Tuple3<Double, Double, Double> testLossRegLossAvgAccuracy = computeLoss(testData, params);

    if (decayRate != 1 && (epochIdx + 1) % decayPeriod == 0) {
      final double prevStepSize = stepSize;
      stepSize *= decayRate;
      LOG.log(Level.INFO, "{0} epochs passed. Step size decays from {1} to {2}",
          new Object[]{decayPeriod, prevStepSize, stepSize});
    }

    return buildEpochResult(trainingLossRegLossAvgAccuracy, testLossRegLossAvgAccuracy);
  }

  /**
   * Pull models one last time and perform validation.
   */
  @Override
  public void cleanup() {
    executor.shutdown();
  }

  /**
   * Pull up-to-date model parameters from server, which become accessible via {@link ModelHolder#getModel()}.
   */
  private void pullModels() {
    final List<Vector> partitions = modelAccessor.pull(classPartitionIndices);
    final Vector[] params = model.getParams();

    for (int classIndex = 0; classIndex < numClasses; ++classIndex) {
      // 0 ~ (numPartitionsPerClass - 1) is for class 0
      // numPartitionsPerClass ~ (2 * numPartitionsPerClass - 1) is for class 1
      // and so on
      final List<Vector> partialModelsForThisClass =
          partitions.subList(classIndex * numPartitionsPerClass, (classIndex + 1) * numPartitionsPerClass);

      // concat partitions into one long vector
      params[classIndex] = vectorFactory.concatDense(partialModelsForThisClass);
    }
  }

  /**
   * Processes one training data instance and update the intermediate model.
   * @param instance training data instance
   * @param threadGradient update for each instance
   */
  private void updateGradient(final MLRData instance, final Vector[] threadGradient) {
    final Vector feature = instance.getFeature();
    final Vector[] params = model.getParams();
    final int label = instance.getLabel();

    // compute h(x, w) = softmax(x dot w)
    final Vector predictions = predict(feature, params);

    // error = h(x, w) - y, where y_j = 1 (if positive for class j) or 0 (otherwise)
    // instead of allocating a new vector for the error,
    // we use the same object for convenience
    predictions.set(label, predictions.get(label) - 1);

    // gradient_j = -stepSize * error_j * x
    if (lambda != 0) {
      for (int j = 0; j < numClasses; ++j) {
        threadGradient[j].axpy(-predictions.get(j) * stepSize, feature);
        threadGradient[j].axpy(-stepSize * lambda, params[j]);
      }
    } else {
      for (int j = 0; j < numClasses; ++j) {
        threadGradient[j].axpy(-predictions.get(j) * stepSize, feature);
      }
    }
  }

  /**
   * Add all the gradients computed by different threads.
   * @param threadGradients list of gradients computed by trainer threads
   * @return an array of vectors each of which is gradient in a class.
   */
  private Vector[] aggregateGradient(final List<Vector[]> threadGradients) {
    final Vector[] gradients = new Vector[numClasses];
    
    for (int classIdx = 0; classIdx < numClasses; classIdx++) {
      gradients[classIdx] = vectorFactory.createDenseZeros(numFeatures);
      for (int threadIdx = 0; threadIdx < numTrainerThreads; threadIdx++) {
        gradients[classIdx].addi(threadGradients.get(threadIdx)[classIdx]);
      }
    }
    return gradients;
  }

  /**
   * Push the gradients to parameter server.
   * @param gradients an array of vectors each of which is gradient in a class.
   */
  private void pushAndResetGradients(final Vector[] gradients) {
    for (int classIndex = 0; classIndex < numClasses; classIndex++) {
      final Vector gradient = gradients[classIndex];

      for (int partitionIndex = 0; partitionIndex < numPartitionsPerClass; ++partitionIndex) {
        final int partitionStart = partitionIndex * numFeaturesPerPartition;
        final int partitionEnd = (partitionIndex + 1) * numFeaturesPerPartition;
        modelAccessor.push(classIndex * numPartitionsPerClass + partitionIndex,
            gradient.slice(partitionStart, partitionEnd));
      }
    }
  }

  /**
   * Compute the loss value using the current models and given data instances.
   * May take long, so do not call frequently.
   */
  private Tuple3<Double, Double, Double> computeLoss(final Collection<MLRData> data, final Vector[] params) {

    double loss = 0;
    int correctPredictions = 0;

    for (final MLRData entry : data) {
      final Vector feature = entry.getFeature();
      final int label = entry.getLabel(); final Vector predictions = predict(feature, params);
      final int prediction = max(predictions).getFirst();

      if (label == prediction) {
        ++correctPredictions;
      }

      loss += -Math.log(predictions.get(label));
    }

    double regLoss = 0;
    if (lambda != 0) {
      // skip this part entirely if lambda is zero, to avoid regularization operation overheads
      for (int classIndex = 0; classIndex < numClasses; ++classIndex) {
        final Vector perClassParams = params[classIndex];
        double l2norm = 0;
        for (int vectorIndex = 0; vectorIndex < perClassParams.length(); ++vectorIndex) {
          l2norm += perClassParams.get(vectorIndex) * perClassParams.get(vectorIndex);
        }
        regLoss += l2norm * lambda / 2;
      }
    }
    regLoss /= numClasses;

    return new Tuple3<>(loss, regLoss, (double) correctPredictions / data.size());
  }

  /**
   * Compute the probability vector of the given data instance, represented by {@code features}.
   */
  private Vector predict(final Vector features, final Vector[] params) {
    final double[] predict = new double[numClasses];
    for (int classIndex = 0; classIndex < numClasses; ++classIndex) {
      predict[classIndex] = params[classIndex].dot(features);
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
  
  private EpochResult buildEpochResult(final Tuple3<Double, Double, Double> traininglossRegLossAvgAccuracy,
                                       final Tuple3<Double, Double, Double> testLossRegLossAvgAccuracy) {
    return EpochResult.newBuilder()
        .addAppMetric(MetricKeys.TRAINING_LOSS, traininglossRegLossAvgAccuracy.getFirst())
        .addAppMetric(MetricKeys.TRAINING_REG_LOSS_AVG, traininglossRegLossAvgAccuracy.getSecond())
        .addAppMetric(MetricKeys.TRAINING_ACCURACY, traininglossRegLossAvgAccuracy.getThird())
        .addAppMetric(MetricKeys.TEST_LOSS, testLossRegLossAvgAccuracy.getFirst())
        .addAppMetric(MetricKeys.TEST_REG_LOSS_AVG, testLossRegLossAvgAccuracy.getSecond())
        .addAppMetric(MetricKeys.TEST_ACCURACY, testLossRegLossAvgAccuracy.getThird())
        .build();
  }
}
