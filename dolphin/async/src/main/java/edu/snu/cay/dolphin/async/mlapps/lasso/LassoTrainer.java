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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.dolphin.async.Trainer;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.dolphin.async.TrainingDataProvider;
import edu.snu.cay.dolphin.async.mlapps.lasso.LassoParameters.*;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.io.network.util.Pair;
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
 * and then randomly picks a dimension to update.
 * The trainer computes and pushes the optimal model value for that particular dimension to
 * minimize the objective function - square loss with l1 regularization.
 *
 * All inner product values are cached in member fields.
 */
final class LassoTrainer implements Trainer {
  private static final Logger LOG = Logger.getLogger(LassoTrainer.class.getName());

  private final int numFeatures;
  private final double lambda;
  private double stepSize;

  private final TrainingDataProvider<Long, LassoData> trainingDataProvider;

  private final Vector oldModel;
  private Vector newModel;

  /**
   * Save vecXArray[i].dot(vecXArray[j]) values in x2x table for caching.
   */
  private final Table<Integer, Integer, Double> x2x = HashBasedTable.create();

  private final VectorFactory vectorFactory;

  /**
   * ParameterWorker object for interacting with the parameter server.
   */
  private final ParameterWorker<Integer, Double, Double> parameterWorker;

  /**
   * Random object to randomly choose an index of the updating feature.
   */
  private final Random random;

  /**
   * Period(iterations) of the printing log of models to check whether the training is working or not.
   */
  private final int printModelPeriod;

  @Inject
  private LassoTrainer(final ParameterWorker<Integer, Double, Double> parameterWorker,
                       @Parameter(Lambda.class) final double lambda,
                       @Parameter(NumFeatures.class) final int numFeatures,
                       @Parameter(StepSize.class) final double stepSize,
                       final TrainingDataProvider<Long, LassoData> trainingDataProvider,
                       final VectorFactory vectorFactory) {
    this.parameterWorker = parameterWorker;
    this.numFeatures = numFeatures;
    this.lambda = lambda;
    this.stepSize = stepSize;
    this.trainingDataProvider = trainingDataProvider;
    this.vectorFactory = vectorFactory;
    this.random = new Random();
    this.printModelPeriod = 50;
    this.oldModel = vectorFactory.createDenseZeros(numFeatures);
  }

  /**
   * {@inheritDoc}
   * Standardize input vectors w.r.t. each feature dimension, as well as the target vector,
   * to have mean 0 and variance 1.
   * Also pre-calculate inner products of input vectors and the target vector, for later use.
   */
  @Override
  public void initialize() {
  }

  /**
   * {@inheritDoc} <br>
   * 1) Pull model from server. <br>
   * 2) Pick dimension to update. <br>
   * 3) Compute the optimal value, (dot(x_i, y) - Sigma_{i != j} (x_i, x_j) * model(j)) / dot(x_i, x_i), where
   *   When computing the optimal value, only compute (x_i, x_j) * model(j) if model(j) != 0, for performance. <br>
   *   Reuse (x_i, x_j) when possible, from {@code x2x}. <br>
   * 4) Push value to server.
   */
  @Override
  public void run(final int iteration) {
    final List<LassoData> totalInstancesProcessed = new LinkedList<>();
    Map<Long, LassoData> nextTrainingData = trainingDataProvider.getNextTrainingData();

    // Model is trained by each mini-batch training data.
    while (!nextTrainingData.isEmpty()) {
      final List<LassoData> instances = new ArrayList<>(nextTrainingData.values());

      // Pull the old model which should be trained in this while loop.
      pullModels();

      /*
       * Transform the instances from LassoData type to the Vector for each feature.
       * Pre-calculate x2y values before we use it.
       * vecXArray is a converted form of training data in the feature order.
       */
      final double[] x2y = new double[numFeatures];
      final int index = random.nextInt(numFeatures);

      final Pair<Vector[], Vector> convertedData = convertDataInFeatureOrder(instances);
      final Vector[] vecXArray = convertedData.getFirst();
      final Vector vecY = convertedData.getSecond();

      for (int i = 0; i < vecXArray.length; i++) {
        x2y[i] = vecXArray[i].dot(vecY);
      }


      // Calculate dotValue(new model[index] value) which will be updated in this mini-batch.
      double dotValue = x2y[index];
      for (int modelIndex = 0; modelIndex < numFeatures; modelIndex++) {
        if (newModel.get(modelIndex) == 0 || index == modelIndex) {
          continue;
        }

        final int min = Math.min(index, modelIndex);
        final int max = Math.max(index, modelIndex);
        if (!x2x.contains(min, max)) {
          x2x.put(min, max, vecXArray[index].dot(vecXArray[modelIndex]));
        }

        dotValue -= x2x.get(min, max) * newModel.get(modelIndex);
      }
      if (!x2x.contains(index, index)) {
        x2x.put(index, index, vecXArray[index].dot(vecXArray[index]));
      }

      dotValue /= x2x.get(index, index);
      newModel.set(index, sthresh(dotValue, lambda, x2x.get(index, index)));

      // Push the new model to the server.
      pushAndResetGradients();

      totalInstancesProcessed.addAll(instances);
      nextTrainingData = trainingDataProvider.getNextTrainingData();
      x2x.clear();
    }

    // Calculate the loss value.
    pullModels();
    final double loss = computeLoss(totalInstancesProcessed);
    LOG.log(Level.INFO, "Loss value: {0}", new Object[]{loss});
    if ((iteration + 1) % printModelPeriod == 0) {
      for (int i = 0; i < numFeatures; i++) {
        LOG.log(Level.INFO, "model : {0}", new Object[]{newModel.get(i)});
      }
    }
  }

  @Override
  public void cleanup() {
  }

  /**
   * Pull up-to-date model parameters from server.
   */
  private void pullModels() {
    for (int modelIndex = 0; modelIndex < numFeatures; modelIndex++) {
      oldModel.set(modelIndex, parameterWorker.pull(modelIndex));
    }
    newModel = oldModel.copy();
  }

  /**
   * Convert data from data order to feature order.
   */
  private Pair<Vector[], Vector> convertDataInFeatureOrder(final List<LassoData> instances) {
    final Vector[] vecXArray = new Vector[numFeatures];
    for (int i = 0; i < numFeatures; i++) {
      vecXArray[i] = vectorFactory.createDenseZeros(instances.size());
    }
    final Vector vecY = vectorFactory.createDenseZeros(instances.size());
    for (int i = 0; i < instances.size(); i++) {
      final Vector feature = instances.get(i).getFeature();
      final double value = instances.get(i).getValue();
      for (int j = 0; j < numFeatures; j++) {
        vecXArray[j].set(i, feature.get(j));
      }
      vecY.set(i, value);
    }
    return new Pair<>(vecXArray, vecY);
  }

  /**
   * Push the gradients to parameter server.
   */
  private void pushAndResetGradients() {
    final Vector gradient = newModel.sub(oldModel);
    for (int modelIndex = 0; modelIndex < numFeatures; ++modelIndex) {
      parameterWorker.push(modelIndex, stepSize * gradient.get(modelIndex));
    }
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

  private double predict(final Vector feature) {
    return newModel.dot(feature);
  }

  private double computeLoss(final List<LassoData> data) {
    double squaredErrorSum = 0;
    double reg = 0;

    for (final LassoData entry : data) {
      final Vector feature = entry.getFeature();
      final double value = entry.getValue();
      final double prediction = predict(feature);
      squaredErrorSum += (value - prediction) * (value - prediction);
    }

    for (int i = 0; i < numFeatures; ++i) {
      reg += newModel.get(i);
    }

    final double loss = 1.0 / (2 * data.size()) * squaredErrorSum + lambda * reg;
    return loss;
  }
}
