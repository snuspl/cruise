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

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.conf.NeuralNetworkConfigurationParameters.*;
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import edu.snu.cay.dolphin.async.dnn.layers.LayerParameter;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static edu.snu.cay.dolphin.async.dnn.blas.MatrixUtils.createOutputMatrix;
import static edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils.*;

/**
 * Neural network model.
 *
 * The input matrix for the neural network model is a collection of input instances.
 * Each column of the matrix represents each input instance.
 */
public final class NeuralNetwork {

  private final MatrixFactory matrixFactory;

  /**
   * A set of layers which a neural network comprises.
   */
  private final LayerBase[] layers;

  /**
   * Parameter worker that provides the updated parameters and gathers gradients for each input instance.
   */
  private final ParameterWorker<Integer, LayerParameter, LayerParameter> parameterWorker;

  /**
   * the empty matrix.
   * This is used as the next error of the last layer's backpropagation.
   */
  private final Matrix emptyMatrix;

  /**
   * The empty layer parameter.
   * This is used as the gradients of layers that are not learnable.
   */
  private final LayerParameter emptyLayerParam;

  /**
   * a list of learnable layer indices.
   * This is used for fetching updated parameters from parameter servers.
   */
  private final List<Integer> learnableLayerIndices;

  /**
   * @param configurationSerializer the serializer to deserialize Tang configurations for layers
   * @param serializedLayerConfSets the set of Tang configurations used to inject layer instances
   * @param inputShape the shape of input data
   * @param parameterWorker the parameter worker for updating layer parameters
   * @param matrixFactory the factory to create new matrices
   * @param injector the injector having the matrix factory configuration to be used for injecting layer instances
   */
  @Inject
  private NeuralNetwork(
      final ConfigurationSerializer configurationSerializer,
      @Parameter(SerializedLayerConfigurationSet.class) final Set<String> serializedLayerConfSets,
      @Parameter(InputShape.class) final String inputShape,
      final ParameterWorker<Integer, LayerParameter, LayerParameter> parameterWorker,
      final MatrixFactory matrixFactory,
      final Injector injector) {
    this.matrixFactory = matrixFactory;
    this.parameterWorker = parameterWorker;
    final Configuration[] layerConfs =
        deserializeLayerConfSetToArray(configurationSerializer, serializedLayerConfSets);
    this.layers = getLayerInstances(injector, layerConfs, inputShape);
    this.emptyMatrix = matrixFactory.create(0);
    this.emptyLayerParam = LayerParameter.newEmptyInstance(matrixFactory);
    this.learnableLayerIndices = getLearnableLayerIndices();
  }

  /**
   * Call after initializing layers.
   */
  @SuppressWarnings("unchecked")
  private List<Integer> getLearnableLayerIndices() {
    final List<Integer> indexList = new ArrayList<>(layers.length); // at most the total number of layers.
    for (final LayerBase layer : layers) {
      if (layer.isLearnable()) {
        indexList.add(layer.getIndex());
      }
    }
    return indexList;
  }

  /**
   * @return the parameters of each layer.
   */
  public LayerParameter[] getParameters() {
    final LayerParameter[] parameters = new LayerParameter[layers.length];
    for (int i = 0; i < layers.length; ++i) {
      if (layers[i].isLearnable()) {
        parameters[i] = layers[i].getLayerParameter();
      }
    }
    return parameters;
  }

  /**
   * Trains neural network with the given input and label.
   * @param input the input matrix.
   * @param label the label matrix.
   */
  public void train(final Matrix input, final Matrix label) {
    updateParameters();

    final Matrix[] activations = ArrayUtils.add(feedForward(input), 0, input); // inserts input at the beginning.
    final Matrix[] errors = backPropagate(activations, label);
    final LayerParameter[] parameterGradients = generateParameterGradients(activations, errors);

    pushGradients(input.getColumns(), parameterGradients);
  }

  /**
   * Trains neural network with the given input and label.
   * @param input the input matrix.
   * @param labels the label array.
   */
  public void train(final Matrix input, final int[] labels) {
    final Matrix labelMatrix = createOutputMatrix(
        matrixFactory, labels, getShapeLength(layers[layers.length - 1].getOutputShape()));
    train(input, labelMatrix);
  }

  /**
   * Pushes parameter gradients to the parameter servers.
   * @param batchSize the number of instance in an input batch
   * @param parameterGradients the list of parameter gradients
   */
  void pushGradients(final int batchSize, final LayerParameter[] parameterGradients) {
    // average parameter gradients
    for (int i = 0; i < parameterGradients.length; ++i) {
      if (layers[i].isLearnable()) {
        parameterWorker.push(i, LayerParameter.newBuilder()
            .setWeightParam(parameterGradients[i].getWeightParam().div(batchSize))
            .setBiasParam(parameterGradients[i].getBiasParam().div(batchSize))
            .build());
      }
    }
  }

  /**
   * Updates the layer parameters by pulling the latest parameters from the parameter servers.
   */
  void updateParameters() {
    final List<LayerParameter> newParameters = parameterWorker.pull(learnableLayerIndices);
    for (int i = 0; i < learnableLayerIndices.size(); ++i) {
      layers[learnableLayerIndices.get(i)].setLayerParameter(newParameters.get(i));
    }
  }

  /**
   * Computes activations from input layer to output layer.
   * @param input the input matrix for input layer.
   * @return an array of activations for each layer.
   */
  public Matrix[] feedForward(final Matrix input) {
    return feedForward(0, layers.length - 1, input);
  }

  /**
   * Computes activations from the specified beginning layer to the specified ending layer.
   * @param begin the index of beginning layer, inclusive.
   * @param end the index of ending layer, inclusive.
   * @param input the input matrix.
   * @return an array of activations for each layer.
   */
  public Matrix[] feedForward(final int begin, final int end, final Matrix input) {
    if (begin > end) {
      throw new IllegalArgumentException(String.format(
          "The beginning index (%d) must be less than or equal to the ending index (%d).", begin, end));
    }

    checkIndices(begin, end, true);

    final Matrix[] activations = new Matrix[end - begin + 1];
    Matrix activation = input;

    for (int i = begin; i <= end; ++i) {
      activation = layers[i].feedForward(activation);
      activations[i - begin] = activation;
    }

    return activations;
  }

  /**
   * Computes errors from the output layer to the input layer.
   * This returns an empty array when the network model has zero or one layer.
   * @param activations an array of activations for each layer.
   * @param label the expected output.
   * @return an array of errors for each layer.
   */
  public Matrix[] backPropagate(final Matrix[] activations,
                                final Matrix label) {
    // Process backpropagation to the second layer.
    // because the error returned by the first layer's backpropagation, is not needed
    // to generate gradients for learning the first layer.
    // The errors for generating gradients used to update the first layer's parameter, are calculated
    // in the next layer's backpropagation.
    return backPropagateTo(1, activations, label);
  }

  /**
   * Computes errors from the output layer to the specified ending layer.
   * This returns an empty array when the network model has zero or one layer.
   * @param end the index of ending layer, inclusive.
   * @param activations an array of activations of each layer.
   * @param label the expected output.
   * @return an array of errors for each layer.
   */
  public Matrix[] backPropagateTo(final int end,
                                  final Matrix[] activations,
                                  final Matrix label) {
    // Case 1: Only one layer
    if (layers.length < 2) {
      // If a neural network has only one layer, the network does not process backpropagation for this layer because
      // this layer cannot generate gradients for updating its parameter.
      // Generating gradients requires the error computed by the next layer.
      return new Matrix[0];
    } else {
      final int lastLayerIndex = layers.length - 1;
      // The first element of activations is the input data.
      // So, (i + 1)-th element of activations refers to the activation of i-th layer.
      final Matrix error = layers[lastLayerIndex].backPropagate(label, activations[lastLayerIndex + 1], emptyMatrix);

      // Case 2: Two layers
      if (lastLayerIndex == end) {
        return new Matrix[]{error};
      // Case 3: More than two layers
      } else {
        return ArrayUtils.add(backPropagateFromTo(lastLayerIndex - 1, end, activations, error), error);
      }
    }
  }

  /**
   * Computes errors from the specified beginning layer to the specified ending layer.
   * @param begin the index of beginning layer, inclusive.
   * @param end the index of ending layer, inclusive.
   * @param activations an array of activations of each layer.
   * @param nextError the error for next layer - the one closer to the output layer.
   * @return an array of errors for each layer.
   */
  public Matrix[] backPropagateFromTo(final int begin, final int end,
                                      final Matrix[] activations,
                                      final Matrix nextError) {
    if (begin == layers.length - 1) {
      throw new IllegalArgumentException("The beginning layer of backPropagateFromTo cannot be the output layer");
    }
    if (end == 0) {
      // The errors for generating gradients of the first layer are calculated by the next layer, not the first layer.
      throw new IllegalArgumentException("The ending layer cannot be the first layer: " +
          "The error that is propagated to the input is unnecessary to generate gradients for the first layer");
    }
    if (begin < end) {
      throw new IllegalArgumentException(String.format(
          "The beginning index (%d) must be greater than or equal to the ending index (%d).", begin, end));
    }

    checkIndices(begin, end, false);

    final Matrix[] errors = new Matrix[begin - end + 1];
    Matrix error = nextError;

    for (int i = begin; i >= end; --i) {
      error = layers[i].backPropagate(activations[i], activations[i + 1], error);
      errors[i - end] = error;
    }
    return errors;
  }

  /**
   * Generates parameter gradients for all layers.
   * @param activations the activation values for each layer.
   * @param errors the errors for each layer.
   * @return an array of parameter gradients for each layer.
   */
  public LayerParameter[] generateParameterGradients(final Matrix[] activations,
                                                     final Matrix[] errors) {
    return generateParameterGradients(0, layers.length - 1, activations, errors);
  }

  /**
   * Generates parameter gradients for each layer from the specified beginning layer to the specified ending layer.
   * @param begin the index of beginning layer, inclusive.
   * @param end the index of ending layer, inclusive.
   * @param activations the activation values for each layer.
   * @param errors the errors for each layer.
   * @return an array of parameter gradients for each layer.
   */
  public LayerParameter[] generateParameterGradients(final int begin, final int end,
                                                     final Matrix[] activations,
                                                     final Matrix[] errors) {
    if (begin > end) {
      throw new IllegalArgumentException(String.format(
          "The beginning index (%d) must be less than or equal to the ending index (%d).", begin, end));
    }

    checkIndices(begin, end, true);

    final LayerParameter[] parameterGradients = new LayerParameter[end - begin + 1];
    for (int i = begin; i <= end; ++i) {
      if (layers[i].isLearnable()) {
        parameterGradients[i - begin] = layers[i].generateParameterGradient(activations[i], errors[i]);
      } else {
        parameterGradients[i - begin] = emptyLayerParam;
      }
    }
    return parameterGradients;
  }

  /**
   * Check whether the indices for the beginning layer and the ending layer are within layer bound.
   * @param begin the index of the beginning layer, inclusive.
   * @param end the index of the ending layer, inclusive.
   * @param isForward the flag for a direction.
   */
  private void checkIndices(final int begin, final int end, final boolean isForward) {
    // Case 1: forward direction
    if (isForward) {
      if (begin < 0) {
        throw new IllegalArgumentException(String.format(
            "The beginning index (%d) must be greater than or equal to 0.", begin));
      }
      if (end >= layers.length) {
        throw new IllegalArgumentException(String.format(
            "The ending index (%d) must be less than the length of layers (%d).", end, layers.length));
      }
      // Case 2: backward direction
    } else {
      if (end < 0) {
        throw new IllegalArgumentException(String.format(
            "The ending index (%d) must be greater than or equal to 0.", end));
      }
      if (begin >= layers.length) {
        throw new IllegalArgumentException(String.format(
            "The beginning index (%d) must be less than the length of layers (%d).", begin, layers.length));
      }
    }
  }
}
