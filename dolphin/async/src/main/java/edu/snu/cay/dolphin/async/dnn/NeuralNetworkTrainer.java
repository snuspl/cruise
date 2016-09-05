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
package edu.snu.cay.dolphin.async.dnn;

import edu.snu.cay.dolphin.async.TrainingDataDivider;
import edu.snu.cay.dolphin.async.Trainer;
import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.data.NeuralNetworkData;
import edu.snu.cay.dolphin.async.dnn.util.Validator;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils.generateIterationLog;

/**
 * Trainer for the neural network job.
 */
final class NeuralNetworkTrainer implements Trainer {

  private static final Logger LOG = Logger.getLogger(NeuralNetworkTrainer.class.getName());

  private final NeuralNetwork neuralNetwork;
  private final Validator crossValidator;
  private final Validator trainingValidator;

  private final MemoryStore<Long> memoryStore;
  private final TrainingDataDivider<Long> trainingDataDivider;

  /**
   * @param neuralNetwork the neural network model
   * @param memoryStore the key-value store for neural network data
   */
  @Inject
  private NeuralNetworkTrainer(final NeuralNetwork neuralNetwork,
                               final MemoryStore<Long> memoryStore,
                               final TrainingDataDivider<Long> trainingDataDivider) {
    this.neuralNetwork = neuralNetwork;
    this.trainingValidator = new Validator(neuralNetwork);
    this.crossValidator = new Validator(neuralNetwork);
    this.memoryStore = memoryStore;
    this.trainingDataDivider = trainingDataDivider;
  }

  @Override
  public void initialize() {

    final Map<Long, NeuralNetworkData> workloadMap = memoryStore.getAll();
    final Collection<NeuralNetworkData> workload = workloadMap.values();

    LOG.log(Level.INFO, "Number of input instances = {0}", workload.size());
  }

  @Override
  public void onEpochStart(final int epoch) {

  }

  @Override
  public void onEpochEnd(final int epoch) {
    LOG.log(Level.INFO, generateIterationLog(
        trainingValidator.getValidationStats(), crossValidator.getValidationStats(), epoch));

    crossValidator.getValidationStats().reset();
    trainingValidator.getValidationStats().reset();
  }

  @Override
  public void run(final int miniBatch) {
    Map<Long, NeuralNetworkData> workloadMap = trainingDataDivider.getNextTrainingDataSplit();
    while (!workloadMap.isEmpty()) {
      final Collection<NeuralNetworkData> workload = workloadMap.values();
      final Collection<NeuralNetworkData> validationWorkload = Collections.emptyList();

      for (final NeuralNetworkData data : workload) {
        final Matrix input = data.getMatrix();
        final int[] labels = data.getLabels();

        if (input.getColumns() != labels.length) {
          throw new RuntimeException("Invalid data: the number of inputs is not equal to the number of labels");
        }

        if (data.isValidation()) {
          validationWorkload.add(data);
        } else {
          neuralNetwork.train(input, labels);
          trainingValidator.validate(input, labels);
        }
      }

      for (final NeuralNetworkData data : validationWorkload) {
        final Matrix input = data.getMatrix();
        final int[] labels = data.getLabels();

        if (input.getColumns() != labels.length) {
          throw new RuntimeException("Invalid data: the number of inputs is not equal to the number of labels");
        }

        crossValidator.validate(input, labels);
      }

      workloadMap = trainingDataDivider.getNextTrainingDataSplit();
    }
  }

  @Override
  public void cleanup() {
  }
}
