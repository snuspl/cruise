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
package edu.snu.cay.dolphin.async.dnn.util;

import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters;
import edu.snu.cay.dolphin.async.dnn.layerparam.initializer.LayerParameterInitializer;
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import edu.snu.cay.dolphin.async.dnn.layers.LayerParameter;
import edu.snu.cay.dolphin.async.dnn.layers.LayerShape;
import org.apache.commons.lang.StringUtils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Utility class for neural network.
 */
public final class NeuralNetworkUtils {

  private NeuralNetworkUtils() {
  }

  /**
   * Delimiter that is used for distinguishing dimensions of shapes.
   */
  private static final String SHAPE_DELIMITER = ",";

  /**
   * Converts a list of integers for a shape to a string.
   * @param layerShape a layer shape.
   * @return a string for a shape.
   */
  public static String shapeToString(final LayerShape layerShape) {
    final List<Integer> dimensionList = new ArrayList<>();
    dimensionList.add(layerShape.getChannel());
    dimensionList.add(layerShape.getWidth());
    dimensionList.add(layerShape.getHeight());
    return StringUtils.join(dimensionList, SHAPE_DELIMITER);
  }

  /**
   * Converts a string for a shape to an array of integers.
   * @param shapeString a string for a shape.
   * @return an array of integers for a shape.
   */
  public static LayerShape shapeFromString(final String shapeString) {
    final String[] inputShapeStrings = shapeString.split(SHAPE_DELIMITER);
    final int[] inputShape = new int[inputShapeStrings.length];
    for (int i = 0; i < inputShapeStrings.length; ++i) {
      inputShape[i] = Integer.parseInt(inputShapeStrings[i]);
    }
    return new LayerShape(inputShape[0], inputShape[1], inputShape[2]);
  }

  public static int getShapeLength(final LayerShape shape) {

    int length = shape.getChannel();
    length *= shape.getWidth();
    length *= shape.getHeight();

    if (length > 0) {
      return length;
    } else {
      throw new IllegalArgumentException("the length of the shape must be positive: " + length);
    }
  }

  /**
   * De-serializes a set of serialized layer configurations to an array of layer configurations.
   * @param configurationSerializer a configuration serializer to deserialize the specified configuration set.
   * @param serializedLayerConfSet a set of serialized layer configurations.
   * @return an array of layer configurations.
   */
  public static Configuration[] deserializeLayerConfSetToArray(
      final ConfigurationSerializer configurationSerializer,
      final Set<String> serializedLayerConfSet) {
    final Configuration[] layerConfigurations = new Configuration[serializedLayerConfSet.size()];
    for (final String serializedLayerConfiguration : serializedLayerConfSet) {
      try {
        final Configuration layerConfiguration = configurationSerializer.fromString(serializedLayerConfiguration);
        final int index = Tang.Factory.getTang().newInjector(layerConfiguration)
            .getNamedInstance(LayerConfigurationParameters.LayerIndex.class);
        layerConfigurations[index] = layerConfiguration;
      } catch (final IOException exception) {
        throw new RuntimeException("IOException while de-serializing layer configuration", exception);
      } catch (final InjectionException exception) {
        throw new RuntimeException("InjectionException", exception);
      }
    }
    return layerConfigurations;
  }

  /**
   * Returns initial layer parameters using the specified injector and the specified array of configurations.
   * This assumes that the specified injector has all parameters that are needed to inject layer parameter initializer
   * instances except the configuration for each layer.
   * @param injector an injector used for injecting layer parameter initializer instances
   * @param layerInitializerConfs an array of configurations for injecting layer parameter initializer instances
   * @param inputShape an input shape for the neural network.
   * @return an array of initial layer parameters
   */
  public static LayerParameter[] getInitialLayerParameters(final Injector injector,
                                                           final Configuration[] layerInitializerConfs,
                                                           final String inputShape) {
    final LayerParameter[] layerParameters = new LayerParameter[layerInitializerConfs.length];

    String currentInputShape = inputShape;
    for (final Configuration layerInitializerConf : layerInitializerConfs) {
      try {
        // bind an input shape for the layer.
        final Configuration finalInitializerConf =
            Tang.Factory.getTang().newConfigurationBuilder(layerInitializerConf)
                .bindNamedParameter(LayerConfigurationParameters.LayerInputShape.class, currentInputShape)
                .build();
        final LayerParameterInitializer layerParameterInitializer =
            injector.forkInjector(finalInitializerConf).getInstance(LayerParameterInitializer.class);
        final int index = layerParameterInitializer.getIndex();

        layerParameters[index] = layerParameterInitializer.generateInitialParameter();
        currentInputShape = shapeToString(layerParameterInitializer.getOutputShape());

      } catch (final InjectionException exception) {
        throw new RuntimeException("InjectionException during injecting LayerParameterInitializer", exception);
      }
    }
    return layerParameters;
  }

  /**
   * Returns layer instances using the specified injector and the specified array of configurations.
   * This assumes that the specified injector has all parameters that are needed to inject layer instances
   * except the configuration for each layer.
   * @param injector an injector used for injecting layer instances
   * @param layerConfs an array of configurations for injecting layer instances
   * @param inputShape an input shape for the neural network.
   * @return an array of layer instances.
   */
  public static LayerBase[] getLayerInstances(final Injector injector,
                                              final Configuration[] layerConfs,
                                              final String inputShape) {
    final LayerBase[] layers = new LayerBase[layerConfs.length];

    String currentInputShape = inputShape;
    for (final Configuration layerConf : layerConfs) {
      try {
        // bind an input shape for the layer.
        final Configuration finalLayerConf = Tang.Factory.getTang().newConfigurationBuilder(layerConf)
            .bindNamedParameter(LayerConfigurationParameters.LayerInputShape.class, currentInputShape)
            .build();
        final LayerBase layer = injector.forkInjector(finalLayerConf).getInstance(LayerBase.class);

        layers[layer.getIndex()] = layer;
        currentInputShape = shapeToString(layer.getOutputShape());

      } catch (final InjectionException exception) {
        throw new RuntimeException("InjectionException while injecting LayerBase", exception);
      }
    }
    return layers;
  }

  private static final String NEWLINE = System.getProperty("line.separator");

  /**
   * Generate the log of statistics for training and cross validation input instances during an iteration.
   * @param trainingValidationStats statistics for training inputs
   * @param crossValidationStats statistics for cross validation inputs
   * @param iteration the index of an iteration
   * @return the generated log string
   */
  public static String generateIterationLog(final ValidationStats trainingValidationStats,
                                            final ValidationStats crossValidationStats,
                                            final int iteration) {
    return new StringBuilder()
        .append(NEWLINE)
        .append("=========================================================")
        .append(NEWLINE)
        .append("Iteration: ")
        .append(iteration)
        .append(NEWLINE)
        .append("Training Error: ")
        .append(trainingValidationStats.getError())
        .append(NEWLINE)
        .append("Cross Validation Error: ")
        .append(crossValidationStats.getError())
        .append(NEWLINE)
        .append("# of training inputs: ")
        .append(trainingValidationStats.getTotalNum())
        .append(NEWLINE)
        .append("# of validation inputs: ")
        .append(crossValidationStats.getTotalNum())
        .append(NEWLINE)
        .append("=========================================================")
        .append(NEWLINE)
        .toString();
  }
}
