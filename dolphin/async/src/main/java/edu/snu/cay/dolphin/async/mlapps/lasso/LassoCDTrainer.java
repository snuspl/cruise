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
 * {@link Trainer} class for the LassoCDREEF application.
 * Based on lasso regression via stochastic coordinate descent, proposed in
 * S. Shalev-Shwartz and A. Tewari, Stochastic Methods for l1-regularized Loss Minimization, 2011.
 *
 * For each iteration, the trainer pulls the whole model from the server,
 * and for each feature, update it's model value(used cyclic version of stochastic coordinate descent).
 * The trainer computes and pushes the optimal model value for that particular dimension to
 * minimize the objective function - square loss with l1 regularization.
 */
final class LassoCDTrainer implements Trainer {
  private static final Logger LOG = Logger.getLogger(LassoCDTrainer.class.getName());

  private List<Integer> partitionIndices;
  private final int numFeaturesPerPartition;
  private final int numFeatures;
  private final int numPartitions;
  private final double lambda;
  private double stepSize;

  private final TrainingDataProvider<Long, LassoData> trainingDataProvider;

  private Vector oldModel;
  private Vector newModel;

  private final VectorFactory vectorFactory;
  private final MatrixFactory matrixFactory;

  private final ParameterWorker<Integer, Vector, Vector> parameterWorker;

  private final double decayRate;
  private final int decayPeriod;

  @Inject
  private LassoCDTrainer(final ParameterWorker<Integer, Vector, Vector> parameterWorker,
                         @Parameter(Lambda.class) final double lambda,
                         @Parameter(NumFeaturesPerPartition.class) final int numFeaturesPerPartition,
                         @Parameter(NumFeatures.class) final int numFeatures,
                         @Parameter(StepSize.class) final double stepSize,
                         @Parameter(DecayRate.class) final double decayRate,
                         @Parameter(DecayPeriod.class) final int decayPeriod,
                         final TrainingDataProvider<Long, LassoData> trainingDataProvider,
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
    this.decayRate = decayRate;
    if (decayRate <= 0.0 || decayRate > 1.0) {
      throw new IllegalArgumentException("decay_rate must be larger than 0 and less than or equal to 1");
    }
    this.decayPeriod = decayPeriod;
    if (decayPeriod <= 0) {
      throw new IllegalArgumentException("decay_period must be a positive value");
    }
  }

  /**
   * {@inheritDoc}
   * Initialize partition indices which will be a key list of a model in EM.
   */
  @Override
  public void initialize() {
    partitionIndices = new ArrayList<>(numPartitions);
    for (int partitionIndex = 0; partitionIndex < numPartitions; ++partitionIndex) {
      partitionIndices.add(partitionIndex);
    }
  }

  /**
   * {@inheritDoc}
   * 1) Pull model from server.
   * 2) For each feature, do 3).
   * 3) Compute the optimal value as follow,
   *    (soft-thresholding function)_(lambda / columnNorm) (getColumn(i)*(y-A_(-i)*x_(-i))/columnNorm)
   *    You can refer more details from Ryan Tibshirani(associate professor of CMU)'s lecture.
   * 4) Push value to server.
   */
  @Override
  public void run(final int iteration) {

    final List<LassoData> totalInstancesProcessed = new LinkedList<>();

    Map<Long, LassoData> nextTrainingData = trainingDataProvider.getNextTrainingData();
    List<LassoData> instances = new ArrayList<>(nextTrainingData.values());

    while (!nextTrainingData.isEmpty()) {
      pullModels();

      final List<Vector> features = new LinkedList<>();
      final Vector yValue = vectorFactory.createDenseZeros(instances.size());
      int iter = 0;
      for (final LassoData instance : instances) {
        features.add(instance.getFeature());
        yValue.set(iter++, instance.getValue());
      }
      final Matrix featureMatrix = matrixFactory.horzcatVecDense(features).transpose();
      final Vector semiPredict = featureMatrix.mmul(newModel);

      for (int i = 0; i < numFeatures; i++) {
        final Vector getColumn = featureMatrix.sliceColumn(i);
        final double getColumnNorm = getColumn.dot(getColumn);
        if (getColumnNorm == 0) {
          continue;
        }
        semiPredict.subi(vectorFactory.createDenseZeros(instances.size()).axpy(newModel.get(i), getColumn));
        newModel.set(i, sthresh((getColumn.dot(yValue.sub(semiPredict))) / getColumnNorm, lambda, getColumnNorm));
        semiPredict.addi(vectorFactory.createDenseZeros(instances.size()).axpy(newModel.get(i), getColumn));
      }

      pushAndResetGradients();

      totalInstancesProcessed.addAll(instances);
      nextTrainingData = trainingDataProvider.getNextTrainingData();
      instances = new ArrayList<>(nextTrainingData.values());
    }

    if (decayRate != 1 && iteration % decayPeriod == 0) {
      final double prevStepSize = stepSize;
      stepSize *= decayRate;
      LOG.log(Level.INFO, "{0} iterations have passed. Step size decays from {1} to {2}",
          new Object[]{decayPeriod, prevStepSize, stepSize});
    }

    pullModels();
    final double loss = computeLoss(totalInstancesProcessed);
    LOG.log(Level.INFO, "Loss value: {0}", new Object[]{loss});
    if ((iteration + 1) % 50 == 0) {
      for (int i = 0; i < numFeatures; i++) {
        LOG.log(Level.INFO, "model : {0}", new Object[]{newModel.get(i)});
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

  private double computeLoss(final List<LassoData> data) {
    double loss = 0;
    double reg = 0;

    for (final LassoData entry : data) {
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
