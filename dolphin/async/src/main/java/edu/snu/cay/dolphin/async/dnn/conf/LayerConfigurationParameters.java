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

/**
 * Parameters for layer configuration.
 *
 * When we need new parameters of new type of layer, we can add them as inner classes of this class.
 */
public final class LayerConfigurationParameters {

  @NamedParameter(doc = "initial bias of a parameter")
  public static final class InitialBias implements Name<Float> {
  }

  @NamedParameter(
      doc = "standard deviation of a normal distribution that is used to generate initial weight of a parameter")
  public static final class InitialWeight implements Name<Float> {
  }

  @NamedParameter(doc = "index of the layer")
  public static final class LayerIndex implements Name<Integer> {
  }

  @NamedParameter(doc = "the shape of input data for layer")
  public static final class LayerInputShape implements Name<String> {
  }

  @NamedParameter(doc = "use of GPU")
  public static final class UseGpu implements Name<Boolean> {
  }

  /**
   * For fully connected layers.
   */
  @NamedParameter(doc = "number of layer output nodes")
  public static final class NumberOfOutput implements Name<Integer> {
  }

  /**
   * For activation layers.
   */
  @NamedParameter(doc = "activation function of layer node")
  public static final class ActivationFunction implements Name<String> {
  }

  @NamedParameter(doc = "loss function of loss layer")
  public static final class LossFunction implements Name<String> {
  }

  /**
   * For lrn layers.
   */
  @NamedParameter(doc = "the number of channels to sum over of lrn layer")
  public static final class LocalSize implements Name<Integer> {
  }

  @NamedParameter(doc = "the scaling parameter of lrn layer")
  public static final class Alpha implements Name<Float> {
  }

  @NamedParameter(doc = "the exponent to raise the power of lrn layer")
  public static final class Beta implements Name<Float> {
  }

  @NamedParameter(doc = "the constant to add of lrn layer")
  public static final class K implements Name<Float> {
  }

  /**
   * For dropout layers.
   */
  @NamedParameter(doc = "probability of dropping neurons of dropout layer")
  public static final class DropoutRatio implements Name<Float> {
  }

  /**
   * For pooling / convolutional layers.
   */
  @NamedParameter(doc = "pooling type of pooling layer")
  public static final class PoolingType implements Name<String> {
  }

  @NamedParameter(doc = "padding height of pooling / convolutional layer")
  public static final class PaddingHeight implements Name<Integer> {
  }

  @NamedParameter(doc = "padding width of pooling / convolutional layer")
  public static final class PaddingWidth implements Name<Integer> {
  }

  @NamedParameter(doc = "stride height of pooling / convolutional layer")
  public static final class StrideHeight implements Name<Integer> {
  }

  @NamedParameter(doc = "stride width of pooling / convolutional layer")
  public static final class StrideWidth implements Name<Integer> {
  }

  @NamedParameter(doc = "kernel height of pooling / convolutional layer")
  public static final class KernelHeight implements Name<Integer> {
  }

  @NamedParameter(doc = "kernel width of pooling / convolutional layer")
  public static final class KernelWidth implements Name<Integer> {
  }
}
