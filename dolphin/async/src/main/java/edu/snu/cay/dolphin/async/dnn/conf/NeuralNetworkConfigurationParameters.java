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

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

import java.util.Set;

/**
 * Parameters for neural network configuration.
 */
public final class NeuralNetworkConfigurationParameters {

  private NeuralNetworkConfigurationParameters() {
  }

  @NamedParameter(doc = "a serialized layer configuration")
  public static final class SerializedLayerConfiguartion implements Name<String> {
  }

  @NamedParameter(doc = "a set of strings that are serializations of network layers")
  public static final class SerializedLayerConfigurationSet implements Name<Set<String>> {
  }

  @NamedParameter(doc = "batch size", default_value = "1")
  public static final class BatchSize implements Name<Integer> {
  }

  @NamedParameter(doc = "step size")
  public static final class StepSize implements Name<Float> {
  }

  @NamedParameter(doc = "the shape of input data")
  public static final class InputShape implements Name<String> {
  }

  @NamedParameter(doc = "random seed that is used to generate random values")
  public static final class RandomSeed implements Name<Long> {
  }

  @NamedParameter(doc = "the device option to run under")
  public static final class CpuOnly implements Name<Boolean> {
  }
}
