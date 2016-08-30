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

import edu.snu.cay.dolphin.async.dnn.layerparam.initializer.ConvolutionalLayerParameterInitializer;
import edu.snu.cay.dolphin.async.dnn.layerparam.initializer.LayerParameterInitializer;
//import edu.snu.cay.dolphin.async.dnn.layers.ConvolutionalLayer;
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import edu.snu.cay.dolphin.async.dnn.layers.cuda.ConvolutionalGpuLayer;
import edu.snu.cay.dolphin.async.dnn.proto.NeuralNetworkProtos;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.util.Builder;

/**
 * Configuration builder for Convolutional layer.
 *
 * The configuration that this builder generates is used to create a convolutional layer instance.
 * The generated configuration needs to bind the implementation for matrix factory and
 * the parameter for a layer input shape, to inject a layer instance.
 */
public final class ConvolutionalLayerConfigurationBuilder implements Builder<Configuration> {

  public static ConvolutionalLayerConfigurationBuilder newConfigurationBuilder() {
    return new ConvolutionalLayerConfigurationBuilder();
  }

  private int paddingHeight = 0;
  private int paddingWidth = 0;
  private int strideHeight = 1;
  private int strideWidth = 1;
  private int kernelHeight;
  private int kernelWidth;
  private float initWeight;
  private float initBias;
  private int numOutput;

  public synchronized ConvolutionalLayerConfigurationBuilder setPaddingHeight(final int paddingHeight) {
    this.paddingHeight = paddingHeight;
    return this;
  }

  public synchronized ConvolutionalLayerConfigurationBuilder setPaddingWidth(final int paddingWidth) {
    this.paddingWidth = paddingWidth;
    return this;
  }

  public synchronized ConvolutionalLayerConfigurationBuilder setStrideHeight(final int strideHeight) {
    this.strideHeight = strideHeight;
    return this;
  }

  public synchronized ConvolutionalLayerConfigurationBuilder setStrideWidth(final int strideWidth) {
    this.strideWidth = strideWidth;
    return this;
  }

  public synchronized ConvolutionalLayerConfigurationBuilder setKernelHeight(final int kernelHeight) {
    this.kernelHeight = kernelHeight;
    return this;
  }

  public synchronized ConvolutionalLayerConfigurationBuilder setKernelWidth(final int kernelWidth) {
    this.kernelWidth = kernelWidth;
    return this;
  }

  public synchronized ConvolutionalLayerConfigurationBuilder setInitWeight(final float initWeight) {
    this.initWeight = initWeight;
    return this;
  }

  public synchronized ConvolutionalLayerConfigurationBuilder setInitBias(final float initBias) {
    this.initBias = initBias;
    return this;
  }

  public synchronized ConvolutionalLayerConfigurationBuilder setNumOutput(final int numOutput) {
    this.numOutput = numOutput;
    return this;
  }


  public synchronized ConvolutionalLayerConfigurationBuilder fromProtoConfiguration(
      final NeuralNetworkProtos.LayerConfiguration protoConf) {
    paddingHeight = protoConf.getConvolutionalParam().getPaddingHeight();
    paddingWidth = protoConf.getConvolutionalParam().getPaddingWidth();
    strideHeight = protoConf.getConvolutionalParam().getStrideHeight();
    strideWidth = protoConf.getConvolutionalParam().getStrideWidth();
    kernelHeight = protoConf.getConvolutionalParam().getKernelHeight();
    kernelWidth = protoConf.getConvolutionalParam().getKernelWidth();
    initWeight = protoConf.getConvolutionalParam().getInitWeight();
    initBias = protoConf.getConvolutionalParam().getInitBias();
    numOutput = protoConf.getConvolutionalParam().getNumOutput();
    return this;
  }

  @Override
  public synchronized Configuration build() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(LayerConfigurationParameters.PaddingHeight.class, String.valueOf(paddingHeight))
        .bindNamedParameter(LayerConfigurationParameters.PaddingWidth.class, String.valueOf(paddingWidth))
        .bindNamedParameter(LayerConfigurationParameters.StrideHeight.class, String.valueOf(strideHeight))
        .bindNamedParameter(LayerConfigurationParameters.StrideWidth.class, String.valueOf(strideWidth))
        .bindNamedParameter(LayerConfigurationParameters.KernelHeight.class, String.valueOf(kernelHeight))
        .bindNamedParameter(LayerConfigurationParameters.KernelWidth.class, String.valueOf(kernelWidth))
        .bindNamedParameter(LayerConfigurationParameters.InitialWeight.class, String.valueOf(initWeight))
        .bindNamedParameter(LayerConfigurationParameters.InitialBias.class, String.valueOf(initBias))
        .bindNamedParameter(LayerConfigurationParameters.NumberOfOutput.class, String.valueOf(numOutput))
        .bindImplementation(LayerBase.class, ConvolutionalGpuLayer.class)
        .bindImplementation(LayerParameterInitializer.class, ConvolutionalLayerParameterInitializer.class)
        .build();
  }
}
