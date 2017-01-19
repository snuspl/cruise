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

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.Trainer;
import edu.snu.cay.dolphin.async.TrainingDataProvider;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.mlapps.lasso.LassoParameters.*;

/**
 * {@link Trainer} class for the Lasso application.
 */
final class LassoTrainerSGD implements Trainer {
  private static final Logger LOG = Logger.getLogger(LassoTrainerSGD.class.getName());

  /**
   * ParameterWorker object used to interact with the parameter server.
   */
  private final ParameterWorker<Integer, Vector, Vector> parameterWorker;

  private final int numFeatures;
  private final int numFeaturesPerPartition;
  private final int numPartitions;

  private double stepSize;

  private final double lambda;

  private final int miniBatchSize;


  private final MemoryStore<Long> memoryStore;
  private final TrainingDataProvider<Long, LassoDataSGD> trainingDataProvider;
  private final VectorFactory vectorFactory;

  private Vector oldModel;
  private Vector newModel;

  private List<Integer> partitionIndices;

  private final double decayRate;
  private final int decayPeriod;

  @Inject
  private LassoTrainerSGD(final ParameterWorker<Integer, Vector, Vector> parameterWorker,
                          @Parameter(NumFeatures.class) final int numFeatures,
                          @Parameter(NumFeaturesPerPartition.class) final int numFeaturesPerPartition,
                          @Parameter(StepSize.class) final double initStepSize,
                          @Parameter(Lambda.class) final double lambda,
                          @Parameter(Parameters.MiniBatchSize.class) final int miniBatchSize,
                          @Parameter(DecayRate.class) final double decayRate,
                          @Parameter(DecayPeriod.class) final int decayPeriod,
                          final MemoryStore<Long> memoryStore,
                          final TrainingDataProvider<Long, LassoDataSGD> trainingDataProvider,
                          final VectorFactory vectorFactory) {
    this.parameterWorker = parameterWorker;
    this.numFeatures = numFeatures;
    this.numFeaturesPerPartition = numFeaturesPerPartition;
    if (numFeatures % numFeaturesPerPartition != 0) {
      throw new RuntimeException("Uneven model partitions");
    }
    this.numPartitions = numFeatures / numFeaturesPerPartition;
    this.stepSize = initStepSize;
    this.lambda = lambda;
    this.miniBatchSize = miniBatchSize;
    this.memoryStore = memoryStore;
    this.trainingDataProvider = trainingDataProvider;
    this.vectorFactory = vectorFactory;
    oldModel = vectorFactory.createDenseZeros(numFeatures);
    newModel = vectorFactory.createDenseZeros(numFeatures);
    this.decayRate = decayRate;
    if (decayRate <= 0.0 || decayRate > 1.0) {
      throw new IllegalArgumentException("decay_rate must be larger than 0 and less than or equal to 1");
    }
    this.decayPeriod = decayPeriod;
    if (decayPeriod <= 0) {
      throw new IllegalArgumentException("decay_period must be a positive value");
    }
  }

  @Override
  public void initialize() {
    partitionIndices = new ArrayList<>(numPartitions);
    for (int partitionIndex = 0; partitionIndex < numPartitions; ++partitionIndex) {
      partitionIndices.add(partitionIndex);
    }
  }

  @Override
  public void run(final int iteration) {

    final List<LassoDataSGD> totalInstancesProcessed = new LinkedList<>();

    Map<Long, LassoDataSGD> nextTrainingData = trainingDataProvider.getNextTrainingData();
    List<LassoDataSGD> instances = new ArrayList<>(nextTrainingData.values());
    while (!nextTrainingData.isEmpty()) {
      pullModels();
      for (final LassoDataSGD instance : instances) {
        updateModel(instance);
      }
      pushAndResetGradients();
      totalInstancesProcessed.addAll(instances);
      nextTrainingData = trainingDataProvider.getNextTrainingData();
      instances = new ArrayList<>(nextTrainingData.values());
    }

    if (!(decayRate == 1) && iteration % decayPeriod == 0) {
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

  @Override
  public void cleanup() {
  }

  private void pullModels() {
    final List<Vector> partialModels = parameterWorker.pull(partitionIndices);
    oldModel = vectorFactory.concatDense(partialModels);
    newModel = oldModel.copy();
  }

  private void updateModel(final LassoDataSGD instance) {
    final Vector feature = instance.getFeature();
    final double value = instance.getValue();
    final double prediction = predict(feature);
    final double diff = prediction - value;

    newModel.axpy(-stepSize * diff, feature);
    newModel.axpy(-lambda * stepSize, sgn(newModel));
  }

  private double predict(final Vector feature) {
    return newModel.dot(feature);
  }

  private int sgn(final double k) {
    if (k > 0) {
      return 1;
    } else if (k == 0) {
      return 0;
    } else {
      return -1;
    }
  }

  private Vector sgn(final Vector model) {
    final Vector sgnVector = vectorFactory.createDenseZeros(numFeatures);
    for (int i = 0; i < numFeatures; ++i) {
      sgnVector.set(i, sgn(model.get(i)));
    }
    return sgnVector;
  }

  private void pushAndResetGradients() {
    final Vector gradient = newModel.sub(oldModel);
    for (int partitionIndex = 0; partitionIndex < numPartitions; ++partitionIndex) {
      final int partitionStart = partitionIndex * numFeaturesPerPartition;
      final int partitionEnd = (partitionIndex + 1) * numFeaturesPerPartition;
      parameterWorker.push(partitionIndex, gradient.slice(partitionStart, partitionEnd));
    }
  }

  private Double computeLoss(final List<LassoDataSGD> data) {
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
