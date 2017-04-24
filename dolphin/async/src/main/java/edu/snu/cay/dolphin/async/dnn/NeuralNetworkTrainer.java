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
package edu.snu.cay.dolphin.async.dnn;

import edu.snu.cay.dolphin.async.*;
import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixUtils;
import edu.snu.cay.dolphin.async.dnn.data.NeuralNetworkData;
import edu.snu.cay.dolphin.async.dnn.data.NeuralNetworkDataParser;
import edu.snu.cay.dolphin.async.dnn.util.Validator;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.exceptions.IdGenerationException;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils.generateEpochLog;

/**
 * Trainer for the neural network job.
 */
final class NeuralNetworkTrainer implements Trainer<NeuralNetworkData> {

  private static final Logger LOG = Logger.getLogger(NeuralNetworkTrainer.class.getName());

  private final NeuralNetworkDataParser dataParser;
  private final NeuralNetwork neuralNetwork;
  private final Validator crossValidator;
  private final Validator trainingValidator;

  private final DataIdFactory<Long> idFactory;
  private final MemoryStore<Long> memoryStore;

  /**
   * @param dataParser the parser that transforms input data into {@link NeuralNetworkData} instances
   * @param neuralNetwork the neural network model
   * @param idFactory the factory that generates ids assigned to neural network data stored in {@link MemoryStore}
   * @param memoryStore the key-value store for neural network data
   */
  @Inject
  private NeuralNetworkTrainer(final NeuralNetworkDataParser dataParser,
                               final NeuralNetwork neuralNetwork,
                               final DataIdFactory<Long> idFactory,
                               final MemoryStore<Long> memoryStore) {
    this.dataParser = dataParser;
    this.neuralNetwork = neuralNetwork;
    this.trainingValidator = new Validator(neuralNetwork);
    this.crossValidator = new Validator(neuralNetwork);
    this.idFactory = idFactory;
    this.memoryStore = memoryStore;
  }

  @Override
  public void initGlobalSettings() {
    // put input data instances into the memory store
    final List<NeuralNetworkData> dataValues = dataParser.get();
    final List<Long> dataKeys;
    try {
      dataKeys = idFactory.getIds(dataValues.size());
    } catch (final IdGenerationException e) {
      throw new RuntimeException("Failed to generate ids for MemoryStore", e);
    }
    memoryStore.putList(dataKeys, dataValues);

    LOG.log(Level.INFO, "Number of input instances = {0}", dataValues.size());
  }

  @Override
  public MiniBatchResult runMiniBatch(final Collection<NeuralNetworkData> miniBatchData,
                                      final Collection<NeuralNetworkData> testSet) {
    for (final NeuralNetworkData data : miniBatchData) {
      if (data.isValidation()) {
        continue;
      }

      final Matrix input = dataParser.asMatrix(data.getInstances());
      final int[] labels = data.getLabels();

      if (input.getColumns() != labels.length) {
        throw new RuntimeException("Invalid data: the number of inputs is not equal to the number of labels");
      }

      neuralNetwork.train(input, labels);
      MatrixUtils.free(input);
    }

    return MiniBatchResult.EMPTY_RESULT;
  }

  @Override
  public EpochResult onEpochFinished(final Collection<NeuralNetworkData> epochData,
                                     final Collection<NeuralNetworkData> trainingSet,
                                     final int epochIdx) {
    // update parameters for model validation
    neuralNetwork.updateParameters();

    for (final NeuralNetworkData data : epochData) {
      final Matrix input = dataParser.asMatrix(data.getInstances());
      final int[] labels = data.getLabels();

      if (data.isValidation()) {
        crossValidator.validate(input, labels);
      } else {
        trainingValidator.validate(input, labels);
      }

      MatrixUtils.free(input);
    }

    LOG.log(Level.INFO, generateEpochLog(
        trainingValidator.getValidationStats(), crossValidator.getValidationStats(), epochIdx));

    crossValidator.getValidationStats().reset();
    trainingValidator.getValidationStats().reset();

    return EpochResult.EMPTY_RESULT;
  }

  @Override
  public void cleanup() {
    neuralNetwork.cleanup();
  }
}
