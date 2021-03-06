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
package edu.snu.spl.cruise.ps.mlapps.lasso;

import edu.snu.spl.cruise.common.math.linalg.Matrix;
import edu.snu.spl.cruise.common.math.linalg.MatrixFactory;
import edu.snu.spl.cruise.common.math.linalg.VectorFactory;
import edu.snu.spl.cruise.common.math.linalg.Vector;
import edu.snu.spl.cruise.ps.CruisePSParameters.*;
import edu.snu.spl.cruise.ps.core.worker.ETModelAccessor;
import edu.snu.spl.cruise.ps.core.worker.ModelAccessor;
import edu.snu.spl.cruise.ps.core.worker.Trainer;
import edu.snu.spl.cruise.services.et.evaluator.api.Table;
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
  private final float lambda;
  private float stepSize;
  private final double decayRate;
  private final int decayPeriod;

  private Vector oldModel;
  private Vector newModel;

  private final VectorFactory vectorFactory;
  private final MatrixFactory matrixFactory;

  private final ModelAccessor<Integer, Vector, Vector> modelAccessor;

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

  @Inject
  private LassoTrainer(final ModelAccessor<Integer, Vector, Vector> modelAccessor,
                       @Parameter(Lambda.class) final float lambda,
                       @Parameter(NumFeatures.class) final int numFeatures,
                       @Parameter(StepSize.class) final float stepSize,
                       @Parameter(DecayRate.class) final double decayRate,
                       @Parameter(DecayPeriod.class) final int decayPeriod,
                       @Parameter(NumFeaturesPerPartition.class) final int numFeaturesPerPartition,
                       final VectorFactory vectorFactory,
                       final MatrixFactory matrixFactory) {
    this.modelAccessor = modelAccessor;
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
  public void runMiniBatch(final Collection<LassoData> miniBatchTrainingData) {
    final int numInstancesToProcess = miniBatchTrainingData.size();

    pullModels();

    // After get feature vectors from each instances, make it concatenate them into matrix for the faster calculation.
    // Pre-calculate sigma_{all j} x_j * model(j) and assign the value into 'preCalculate' vector.
    final Pair<Matrix, Vector> featureMatrixAndValues = convertToFeaturesAndValues(miniBatchTrainingData);
    final Matrix featureMatrix = featureMatrixAndValues.getLeft();
    final Vector yValues = featureMatrixAndValues.getRight();

    final Vector preCalculate = featureMatrix.mmul(newModel);

    final Vector columnVector = vectorFactory.createDenseZeros(numInstancesToProcess);
    // For each dimension, compute the optimal value.
    for (int featureIdx = 0; featureIdx < numFeatures; featureIdx++) {
      if (closeToZero(newModel.get(featureIdx))) {
        continue;
      }
      for (int rowIdx = 0; rowIdx < numInstancesToProcess; rowIdx++) {
        columnVector.set(rowIdx, featureMatrix.get(rowIdx, featureIdx));
      }
      final double columnNorm = columnVector.dot(columnVector);
      if (closeToZero(columnNorm)) {
        continue;
      }
      preCalculate.subi(columnVector.scale(newModel.get(featureIdx)));
      newModel.set(featureIdx,
          (float) sthresh((columnVector.dot(yValues.sub(preCalculate))) / columnNorm, lambda, columnNorm));
      preCalculate.addi(columnVector.scale(newModel.get(featureIdx)));
    }

    pushGradients();
  }

  @Override
  public void onEpochFinished(final int epochIdx) {
    if ((epochIdx + 1) % PRINT_MODEL_PERIOD == 0) {
      for (int i = 0; i < numFeatures; i++) {
        LOG.log(Level.INFO, "model : {0}", newModel.get(i));
      }
    }

    if (decayRate != 1 && (epochIdx + 1) % decayPeriod == 0) {
      final double prevStepSize = stepSize;
      stepSize *= decayRate;
      LOG.log(Level.INFO, "{0} epochs have passed. Step size decays from {1} to {2}",
          new Object[]{decayPeriod, prevStepSize, stepSize});
    }
  }

  @Override
  public Map<CharSequence, Double> evaluateModel(final Collection<LassoData> inputData,
                                                 final Collection<LassoData> testData,
                                                 final Table modelTable) {
    // Calculate the loss value.
    pullModels(modelTable);

    final double trainingLoss = computeLoss(inputData);
    final double testLoss = computeLoss(testData);

    LOG.log(Level.INFO, "Training Loss: {0}, Test Loss: {1}", new Object[] {trainingLoss, testLoss});

    final Map<CharSequence, Double> map = new HashMap<>();
    map.put("training_loss", trainingLoss);
    map.put("test_loss", trainingLoss);

    return map;
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
    final List<Vector> partialModels = modelAccessor.pull(modelPartitionIndices);
    oldModel = vectorFactory.concatDense(partialModels);
    newModel = oldModel.copy();
  }

    /**
   * Pull up-to-date model parameters from server.
   */
  private void pullModels(final Table modelTable) {
    final List<Vector> partialModels = ((ETModelAccessor) modelAccessor).pull(modelPartitionIndices, modelTable);
    oldModel = vectorFactory.concatDense(partialModels);
    newModel = oldModel.copy();
  }

  /**
   * Push the gradients to parameter server.
   */
  private void pushGradients() {
    final Vector gradient = newModel.sub(oldModel);
    for (int partitionIndex = 0; partitionIndex < numPartitions; ++partitionIndex) {
      final int partitionStart = partitionIndex * numFeaturesPerPartition;
      final int partitionEnd = (partitionIndex + 1) * numFeaturesPerPartition;
      modelAccessor.push(partitionIndex, vectorFactory.createDenseZeros(numFeaturesPerPartition)
          .axpy(stepSize, gradient.slice(partitionStart, partitionEnd)));
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

  /**
   * @return {@code true} if the value is close to 0.
   */
  private boolean closeToZero(final double value) {
    return Math.abs(value) < ZERO_THRESHOLD;
  }
}
