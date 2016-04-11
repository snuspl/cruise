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
package edu.snu.cay.async.examples.mlr;

import edu.snu.cay.async.examples.mlr.MLRREEF.*;
import edu.snu.cay.async.Worker;
import edu.snu.cay.async.WorkerSynchronizer;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link Worker} class for the MLRREEF application.
 * Uses {@code numClasses} model vectors to determine which class each data instance belongs to.
 * The model vector that outputs the highest dot product value is declared as that data instance's prediction.
 */
final class MLRWorker implements Worker {
  private static final Logger LOG = Logger.getLogger(MLRWorker.class.getName());

  /**
   * Parser object for fetching and parsing the input dataset.
   */
  private final MLRParser mlrParser;

  /**
   * Synchronization component for setting a global barrier across workers.
   */
  private final WorkerSynchronizer synchronizer;

  /**
   * Worker object used to interact with the parameter server.
   */
  private final ParameterWorker<Integer, Vector, Vector> worker;

  /**
   * Number of possible classes for a data instance.
   */
  private final int numClasses;

  /**
   * Number of features for each data instance.
   */
  private final int numFeatures;

  /**
   * Number of data instances to process before pushing gradients to the server.
   */
  private final int batchSize;

  /**
   * Size of each step taken during gradient descent.
   */
  private final double stepSize;

  /**
   * L2 regularization constant.
   */
  private final double lambda;

  /**
   * Number of iterations to wait until logging the loss value.
   */
  private final int lossLogPeriod;

  /**
   * Object for creating {@link Vector} instances.
   */
  private final VectorFactory vectorFactory;

  /**
   * The input dataset, given as a list of pairs which are in the form, (input vector, label).
   */
  private List<Pair<Vector, Integer>> data;

  /**
   * Model vectors that represent each class.
   */
  private List<Vector> models;

  /**
   * Cumulated gradients that will be pushed to the server.
   */
  private List<Vector> gradient;

  /**
   * A list from 0 to (numClasses-1) that will be used during {@code worker.pull()}.
   */
  private List<Integer> classIndices;

  /**
   * Number of times {@code run()} has been called.
   */
  private int iteration;

  @Inject
  private MLRWorker(final MLRParser mlrParser,
                    final WorkerSynchronizer synchronizer,
                    final ParameterWorker<Integer, Vector, Vector> worker,
                    @Parameter(NumClasses.class) final int numClasses,
                    @Parameter(NumFeatures.class) final int numFeatures,
                    @Parameter(BatchSize.class) final int batchSize,
                    @Parameter(StepSize.class) final double stepSize,
                    @Parameter(Lambda.class) final double lambda,
                    @Parameter(LossLogPeriod.class) final int lossLogPeriod,
                    final VectorFactory vectorFactory) {
    this.mlrParser = mlrParser;
    this.synchronizer = synchronizer;
    this.worker = worker;
    this.numClasses = numClasses;
    this.numFeatures = numFeatures;
    this.batchSize = batchSize;
    this.stepSize = stepSize;
    this.lambda = lambda;
    this.lossLogPeriod = lossLogPeriod;
    this.vectorFactory = vectorFactory;
    this.iteration = 0;
  }

  /**
   * Parse the input dataset, initialize a few data structures, and wait for other workers.
   */
  @Override
  public void initialize() {
    data = mlrParser.parse();

    gradient = new ArrayList<>(numClasses);
    classIndices = new ArrayList<>(numClasses);
    for (int classIndex = 0; classIndex < numClasses; ++classIndex) {
      // initialize gradients to zero vectors
      gradient.add(vectorFactory.createDenseZeros(numFeatures));
      classIndices.add(classIndex);
    }

    // all workers should start at the same time
    synchronizer.globalBarrier();
  }

  @Override
  public void run() {
    models = worker.pull(classIndices);

    int numInstances = 0;
    for (final Pair<Vector, Integer> entry : data) {
      if (numInstances >= batchSize) {
        // push gradients and pull fresh models
        refreshModel();
        numInstances = 0;
      }

      final Vector features = entry.getFirst();
      final int label = entry.getSecond();

      // compute h(x, w) = softmax(x dot w)
      final Vector predictions = predict(features);

      // error = h(x, w) - y, where y_j = 1 (if positive for class j) or 0 (otherwise)
      // instead of allocating a new vector for the error,
      // we use the same object for convenience
      predictions.set(label, predictions.get(label) - 1);

      // gradient_j = -stepSize * error_j * x
      // stepSize is multiplied at pushAndResetGradients()
      for (int j = 0; j < numClasses; ++j) {
        gradient.get(j).axpy(-predictions.get(j), features);
      }
      ++numInstances;
    }

    if (numInstances > 0) {
      // flush gradients for remaining instances to server
      pushAndResetGradients();
    }

    ++iteration;
    if (lossLogPeriod > 0 && iteration % lossLogPeriod == 0) {
      LOG.log(Level.INFO, "Iteration: {0}", iteration);
      LOG.log(Level.INFO, "Loss: {0}", computeLoss());
    }
  }

  /**
   * Pull models one last time and perform validation.
   */
  @Override
  public void cleanup() {
    synchronizer.globalBarrier();

    models = worker.pull(classIndices);
    int numInstances = 0;
    int correctPredictions = 0;
    for (final Pair<Vector, Integer> entry : data) {
      final Vector features = entry.getFirst();
      final int label = entry.getSecond();
      final int prediction = max(predict(features)).getFirst();

      ++numInstances;
      if (label == prediction) {
        ++correctPredictions;
      }
    }

    LOG.log(Level.INFO, "Number of instances: {0}", numInstances);
    LOG.log(Level.INFO, "Correct predictions on training dataset: {0}", correctPredictions);
    LOG.log(Level.INFO, "Prediction accuracy on training dataset: {0}", (float) correctPredictions / numInstances);
  }

  private void refreshModel() {
    pushAndResetGradients();
    models = worker.pull(classIndices);
  }

  private void pushAndResetGradients() {
    for (int i = 0; i < numClasses; i++) {
      // gradient_j = -stepSize * error_j * x
      // error * x is multiplied at run()
      gradient.get(i).scalei(stepSize / batchSize);

      // gradient for regularization
      gradient.get(i).axpy(-stepSize * lambda, models.get(i));

      worker.push(i, gradient.get(i));

      // reset this gradient to zero
      gradient.set(i, vectorFactory.createDenseZeros(numFeatures));
    }
  }

  /**
   * Compute the loss value using the current models and all data instances.
   * May take long, so do not call frequently.
   */
  private double computeLoss() {
    double loss = 0;
    int numInstances = 0;
    for (final Pair<Vector, Integer> entry : data) {
      final Vector features = entry.getFirst();
      final int label = entry.getSecond();
      final Vector predictions = predict(features);

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

    for (int classIndex = 0; classIndex < numClasses; ++classIndex) {
      final Vector model = models.get(classIndex);
      double l2norm = 0;
      for (int vectorIndex = 0; vectorIndex < model.length(); ++vectorIndex) {
        l2norm += model.get(vectorIndex) * model.get(vectorIndex);
      }
      loss += l2norm * lambda / 2;
    }
    loss /= numClasses;

    return loss;
  }

  /**
   * Compute the probability vector of the given data instance, represented by {@code features}.
   */
  private Vector predict(final Vector features) {
    final double[] predict = new double[models.size()];
    for (int i = 0; i < models.size(); i++) {
      predict[i] = models.get(i).dot(features);
    }
    return softmax(vectorFactory.createDense(predict));
  }

  private static Vector softmax(final Vector vector) {
    // prevent overflow during exponential operations
    // https://lingpipe-blog.com/2009/06/25/log-sum-of-exponentials/
    final double logSumExp = logSumExp(vector);
    for (int index = 0; index < vector.length(); ++index) {
      vector.set(index, Math.min(1, Math.exp(vector.get(index) - logSumExp)));
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
}
