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
import edu.snu.cay.dolphin.async.Trainer;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.dolphin.async.TrainingDataProvider;
import edu.snu.cay.dolphin.async.mlapps.lasso.LassoParameters.*;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
final class LassoTrainer implements Trainer {
  private static final Logger LOG = Logger.getLogger(LassoTrainer.class.getName());

  private final int numFeatures;
  private final double lambda;
  private double stepSize;

  private final TrainingDataProvider<Long, LassoData> trainingDataProvider;

  private Vector oldModel;
  private Vector newModel;

  private final VectorFactory vectorFactory;
  private final MatrixFactory matrixFactory;

  /**
   * ParameterWorker object for interacting with the parameter server.
   */
  private final ParameterWorker<Integer, Double, Double> parameterWorker;

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
                       final VectorFactory vectorFactory,
                       final MatrixFactory matrixFactory) {
    this.parameterWorker = parameterWorker;
    this.numFeatures = numFeatures;
    this.lambda = lambda;
    this.stepSize = stepSize;
    this.trainingDataProvider = trainingDataProvider;
    this.vectorFactory = vectorFactory;
    this.matrixFactory = matrixFactory;
    this.printModelPeriod = 50;
  }

  @Override
  public void initialize() {
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
  public void run(final int iteration, final AtomicBoolean abortFlag) {
    final List<LassoData> totalInstancesProcessed = new LinkedList<>();
    Map<Long, LassoData> nextTrainingData = trainingDataProvider.getNextTrainingData();

    // Model is trained by each mini-batch training data.
    while (!nextTrainingData.isEmpty()) {
      final List<LassoData> instances = new ArrayList<>(nextTrainingData.values());

      // Pull the old model which should be trained in this while loop.
      pullModels();

      // After get feature vectors from each instances, make it concatenate them into matrix for the faster calculation.
      // Pre-calculate sigma_{all j} x_j * model(j) and assign the value into precalcuate vector.
      final List<Vector> features = new LinkedList<>();
      final Vector yValue = vectorFactory.createDenseZeros(instances.size());
      int iter = 0;
      for (final LassoData instance : instances) {
        features.add(instance.getFeature());
        yValue.set(iter++, instance.getValue());
      }
      final Matrix featureMatrix = matrixFactory.horzcatVecDense(features).transpose();
      final Vector precalculate = featureMatrix.mmul(newModel);

      // For each dimension, compute the optimal value.
      for (int i = 0; i < numFeatures; i++) {
        final Vector getColumn = featureMatrix.sliceColumn(i);
        final double getColumnNorm = getColumn.dot(getColumn);
        if (getColumnNorm == 0) {
          continue;
        }
        precalculate.subi(getColumn.scale(newModel.get(i)));
        newModel.set(i, sthresh((getColumn.dot(yValue.sub(precalculate))) / getColumnNorm, lambda, getColumnNorm));
        precalculate.addi(getColumn.scale(newModel.get(i)));
      }

      // Push the new model to the server.
      pushAndResetGradients();

      totalInstancesProcessed.addAll(instances);
      nextTrainingData = trainingDataProvider.getNextTrainingData();
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
    oldModel = vectorFactory.createDenseZeros(numFeatures);
    for (int modelIndex = 0; modelIndex < numFeatures; modelIndex++) {
      oldModel.set(modelIndex, parameterWorker.pull(modelIndex));
    }
    newModel = oldModel.copy();
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

  /**
   * Predict the y value for the feature value.
   */
  private double predict(final Vector feature) {
    return newModel.dot(feature);
  }

  /**
   * Compute the loss value for the data.
   */
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
