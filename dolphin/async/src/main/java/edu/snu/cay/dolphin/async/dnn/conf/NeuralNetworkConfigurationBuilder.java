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
package edu.snu.cay.dolphin.async.dnn.conf;

import edu.snu.cay.dolphin.async.dnn.conf.NeuralNetworkConfigurationParameters.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.Builder;

import java.util.ArrayList;
import java.util.List;

import static edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils.shapeToString;

/**
 * Configuration builder for neural network.
 *
 * The configuration that this builder generates is used to create a neural network instance.
 * The generated configuration should be merged with the BLAS configuration.
 */
public final class NeuralNetworkConfigurationBuilder implements Builder<Configuration> {

  private List<Configuration> layerConfigurations = new ArrayList<>();
  private ConfigurationSerializer configurationSerializer = new AvroConfigurationSerializer();
  private float stepSize = 1e-2f;
  private String inputShape;
  private int batchSize = 1;
  private long randomSeed = System.currentTimeMillis();
  private boolean cpuOnly = true;

  public static NeuralNetworkConfigurationBuilder newConfigurationBuilder() {
    return new NeuralNetworkConfigurationBuilder();
  }

  public synchronized NeuralNetworkConfigurationBuilder addLayerConfiguration(final Configuration layerConfiguration) {
    layerConfigurations.add(layerConfiguration);
    return this;
  }

  public synchronized NeuralNetworkConfigurationBuilder setStepSize(final float stepSize) {
    this.stepSize = stepSize;
    return this;
  }

  public synchronized NeuralNetworkConfigurationBuilder setInputShape(final List<Integer> inputShapeList) {
    this.inputShape = shapeToString(inputShapeList);
    return this;
  }

  public synchronized NeuralNetworkConfigurationBuilder setInputShape(final int... inputShape) {
    this.inputShape = shapeToString(inputShape);
    return this;
  }

  public synchronized NeuralNetworkConfigurationBuilder setBatchSize(final int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public synchronized NeuralNetworkConfigurationBuilder setRandomSeed(final long randomSeed) {
    this.randomSeed = randomSeed;
    return this;
  }

  public synchronized NeuralNetworkConfigurationBuilder setCpuOnly(final boolean cpuOnly) {
    this.cpuOnly = cpuOnly;
    return this;
  }

  @Override
  public synchronized Configuration build() {
    final JavaConfigurationBuilder jb = Tang.Factory.getTang().newConfigurationBuilder();

    for (int i = 0; i < layerConfigurations.size(); ++i) {
      final Configuration finalLayerConfiguration =
          Tang.Factory.getTang().newConfigurationBuilder(layerConfigurations.get(i))
              .bindNamedParameter(LayerConfigurationParameters.LayerIndex.class, String.valueOf(i))
              .build();

      jb.bindSetEntry(SerializedLayerConfigurationSet.class,
          configurationSerializer.toString(finalLayerConfiguration));
    }

    jb.bindNamedParameter(StepSize.class, String.valueOf(stepSize));
    jb.bindNamedParameter(InputShape.class, inputShape);
    jb.bindNamedParameter(BatchSize.class, String.valueOf(batchSize));
    jb.bindNamedParameter(RandomSeed.class, String.valueOf(randomSeed));
    jb.bindNamedParameter(CpuOnly.class, String.valueOf(cpuOnly));

    return jb.build();
  }
}
