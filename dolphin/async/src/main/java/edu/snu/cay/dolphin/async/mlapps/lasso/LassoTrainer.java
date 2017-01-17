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

  private List<Integer> partitionIndices;
  private final int numFeaturesPerPartition;
  private final int numFeatures;
  private final int numPartitions;
  private final double lambda;
  private final double stepSize;

  private final TrainingDataProvider<Long, LassoDataSGD> trainingDataProvider;

  private Vector oldModel;
  private Vector newModel;

  private final VectorFactory vectorFactory;
  private final MatrixFactory matrixFactory;

  /**
   * ParameterWorker object for interacting with the parameter server.
   */
  private final ParameterWorker<Integer, Vector, Vector> parameterWorker;

  @Inject
  private LassoTrainer(final ParameterWorker<Integer, Vector, Vector> parameterWorker,
                       @Parameter(Lambda.class) final double lambda,
                       @Parameter(NumFeaturesPerPartition.class) final int numFeaturesPerPartition,
                       @Parameter(NumFeatures.class) final int numFeatures,
                       @Parameter(StepSize.class) final double stepSize,
                       final TrainingDataProvider<Long, LassoDataSGD> trainingDataProvider,
                       final VectorFactory vectorFactory,
                       final MatrixFactory matrixFactory) {
    this.parameterWorker = parameterWorker;
    this.numFeaturesPerPartition = numFeaturesPerPartition;
    this.numFeatures = numFeatures;
    if (numFeatures % numFeaturesPerPartition != 0) {
      throw new RuntimeException("Uneven model partitions");
    }
    this.numPartitions =  numFeatures / numFeaturesPerPartition;
    this.lambda = lambda;
    this.stepSize = stepSize;
    this.trainingDataProvider = trainingDataProvider;
    this.vectorFactory = vectorFactory;
    this.matrixFactory = matrixFactory;
  }

  /**
   * {@inheritDoc}
   * Standardize input vectors w.r.t. each feature dimension, as well as the target vector,
   * to have mean 0 and variance 1.
   * Also pre-calculate inner products of input vectors and the target vector, for later use.
   */
  @Override
  public void initialize() {
    partitionIndices = new ArrayList<>(numPartitions);
    for (int partitionIndex = 0; partitionIndex < numPartitions; ++partitionIndex) {
      partitionIndices.add(partitionIndex);
    }
  }

  /**
   * {@inheritDoc} <br>
   * 1) Pull model from server. <br>
   * 2) Pick dimension to update. <br>
   * 3) Compute the optimal value, (dot(x_i, y) - Sigma_{i != j} (x_i, x_j) * model(j)) / N, where
   *   N equals the number of instances, i.e. the length of y. <br>
   * - When computing the optimal value, only compute (x_i, x_j) * model(j) if model(j) != 0, for performance. <br>
   * - Reuse (x_i, x_j) when possible, from {@code x2x}. <br>
   * 4) Push value to server.
   */
  @Override
  public void run(final int iteration) {

    final List<LassoDataSGD> totalInstancesProcessed = new LinkedList<>();

    Map<Long, LassoDataSGD> nextTrainingData = trainingDataProvider.getNextTrainingData();
    List<LassoDataSGD> instances = new ArrayList<>(nextTrainingData.values());

    while (!nextTrainingData.isEmpty()) {
      pullModels();

      final List<Vector> features = new LinkedList<>();
      final Vector yValue = vectorFactory.createDenseZeros(instances.size());
      int iter = 0;
      for (final LassoDataSGD instance : instances) {
        features.add(instance.getFeature());
        yValue.set(iter++, instance.getValue());
      }
      final Matrix featureMatrix = matrixFactory.horzcatVecDense(features).transpose();
      final Vector semiPredict = featureMatrix.mmul(newModel);

      for (int i = 0; i < numFeatures; i++) {
        final Vector getColumn = featureMatrix.sliceColumn(i);
        final double getColumnNorm = getColumn.dot(getColumn);
        semiPredict.subi(vectorFactory.createDenseZeros(instances.size()).axpy(newModel.get(i), getColumn));
        newModel.set(i, sthresh((getColumn.dot(yValue.sub(semiPredict))) / getColumnNorm, lambda, getColumnNorm));
        semiPredict.addi(vectorFactory.createDenseZeros(instances.size()).axpy(newModel.get(i), getColumn));
      }

      pushAndResetGradients();

      totalInstancesProcessed.addAll(instances);
      nextTrainingData = trainingDataProvider.getNextTrainingData();
      instances = new ArrayList<>(nextTrainingData.values());
    }

    pullModels();
    final double loss = computeLoss(totalInstancesProcessed);
    LOG.log(Level.INFO, "Loss value: {0}", loss);
    if ((iteration + 1) % 50 == 0) {
      for (int i = 0; i < numFeatures; i++) {
        LOG.log(Level.INFO, "model : {0}", newModel.get(i));
      }
    }

  }

  /**
   * {@inheritDoc}
   * Pull and log the whole model.
   */
  @Override
  public void cleanup() {
  }

  private void pullModels() {
    final List<Vector> partialModels = parameterWorker.pull(partitionIndices);
    oldModel = vectorFactory.concatDense(partialModels);
    newModel = oldModel.copy();
  }

  private void pushAndResetGradients() {
    final Vector gradient = newModel.sub(oldModel);
    for (int partitionIndex = 0; partitionIndex < numPartitions; ++partitionIndex) {
      final int partitionStart = partitionIndex * numFeaturesPerPartition;
      final int partitionEnd = (partitionIndex + 1) * numFeaturesPerPartition;
      parameterWorker.push(partitionIndex, vectorFactory.createDenseZeros(numFeaturesPerPartition)
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
  private double predict(final Vector feature) {
    return newModel.dot(feature);
  }

  private double computeLoss(final List<LassoDataSGD> data) {
    double loss = 0;
    double reg = 0;

    for (final LassoDataSGD entry : data) {
      final Vector feature = entry.getFeature();
      final double value = entry.getValue();
      final double prediction = predict(feature);
      loss += (value - prediction) * (value - prediction);
    }

    for (int i = 0; i < numFeatures; ++i) {
      reg += newModel.get(i);
    }

    loss = 1.0 / (2 * data.size()) * loss + lambda * reg;

    return loss;
  }
}
