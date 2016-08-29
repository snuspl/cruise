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
package edu.snu.cay.dolphin.async.dnn.layers.cuda;

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.blas.cuda.MatrixCudaFactory;
import edu.snu.cay.dolphin.async.dnn.conf.ConvolutionalLayerConfigurationBuilder;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.LayerIndex;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.LayerInputShape;
import edu.snu.cay.dolphin.async.dnn.conf.NeuralNetworkConfigurationParameters;
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import edu.snu.cay.dolphin.async.dnn.layers.LayerParameter;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static edu.snu.cay.dolphin.async.dnn.layers.LayerParameterUtils.compare;
import static org.junit.Assert.assertTrue;

/**
 * Test class for gpu convolutional layer.
 */
public class ConvolutionalGpuLayerTest {

  private static MatrixFactory matrixFactory;
  private static final float TOLERANCE = 1e-4f;
  private static final Configuration MATRIX_CONF = Tang.Factory.getTang().newConfigurationBuilder()
      .bindImplementation(MatrixFactory.class, MatrixCudaFactory.class)
      .build();

  static {
    try {
      matrixFactory = Tang.Factory.getTang().newInjector(MATRIX_CONF).getInstance(MatrixFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("InjectionException while injecting a matrix factory: " + e);
    }
  }

  private final Matrix input = matrixFactory.create(new float[][]{
      {0, 10},
      {9, 22},
      {2, 13},
      {8, 0},
      {3, 5},
      {7, 3},
      {10, 6},
      {4, 7},
      {1, 9}});
  private final Matrix expectedConvolutionalActivation = matrixFactory.create(new float[][]{
      {0.406522f, -2.8708f},
      {-0.059460f, -4.5747f},
      {-0.238702f, 1.6502f},
      {-0.760506f, 1.4035f}});
  private final Matrix expectedConvolutionalWithPaddingActivation = matrixFactory.create(new float[][]{
      {0.00000f, 2.4788705f},
      {2.23098f, 6.1111124f},
      {1.08761f, 4.6692458f},
      {0.13152f, 0.8548766f},
      {1.98310f, -0.9591301f},
      {0.40652f, -2.8707870f},
      {-0.05946f, -4.5747085f},
      {0.06029f, -2.4028976f},
      {1.71157f, 1.4873223f},
      {-0.23870f, 1.6502027f},
      {-0.76051f, 1.4034946f},
      {-1.33434f, -0.0082032f},
      {-0.95913f, -0.5754781f},
      {-2.38379f, -1.8714727f},
      {-0.89597f, -2.2633123f},
      {-0.20001f, -1.8001224f}});
  private final Matrix nextError = matrixFactory.create(new float[][]{
      {0.1f, 0},
      {0.3f, 0.3f},
      {0.6f, 0.4f},
      {0.4f, 0.1f}});
  private final Matrix nextErrorWithPadding = matrixFactory.create(new float[][]{
      {0.2f, 0},
      {0.1f, 0.4f},
      {0.4f, 1.2f},
      {0.8f, 0.1f},
      {0.2f, 0},
      {0, 0.3f},
      {0.4f, 1.3f},
      {0.7f, 0.8f},
      {0.8f, 0.6f},
      {0.1f, 0.5f},
      {0, 0.1f},
      {0.2f, 0.2f},
      {0.2f, 0.5f},
      {0.1f, 0},
      {0.7f, 0.2f},
      {0.5f, 0.3f}});
  private final Matrix expectedConvolutionalError = matrixFactory.create(new float[][]{
      {-0.0200014f, 0},
      {-0.0695954f, -0.0600042f},
      {-0.0287739f, -0.0287739f},
      {-0.1134325f, -0.0800056f},
      {-0.0930369f, -0.0386389f},
      {0.03600091f, 0.06477481f},
      {0.0394554f, 0.0263036f},
      {0.1750358f, 0.1057307f},
      {0.0991548f, 0.0247887f}});
  private final Matrix expectedConvolutionalWithPaddingError = matrixFactory.create(new float[][]{
      {0.0369707f, -0.0337006f},
      {-0.0289132f, -0.110726f},
      {-0.0266129f, 0.0193425f},
      {-0.0471544f, -0.137827f},
      {0.0167122f, 0.0918946f},
      {0.105183f, 0.325266f},
      {0.165702f, 0.133655f},
      {-0.124812f, 0.0905167f},
      {-0.153994f, -0.0412462f}});
  private final Matrix input3D = matrixFactory.create(new float[][]{
      {0, 10, 10, 2, 0, 5,
          1, 5, 0, 1, 10, 1}}).transpose();
  private final Matrix expectedConvolutional3DActivation = matrixFactory.create(new float[][]{
      {0.462375f, -1.040396f, -1.410072f, -1.575518f,
          -0.703437f, -2.622436f, -6.148920f, 1.164392f}}).transpose();
  private final Matrix nextError3D = matrixFactory.create(new float[][]{
      {0, 0.1f, 0.2f, 0.1f,
          0.4f, 0.1f, 0, 0.2f}}).transpose();
  private final Matrix expectedConvolutional3DError = matrixFactory.create(new float[][]{
      {-0.000347f, -0.047422f, -0.017259f, -0.062726f, 0.017144f, 0.083920f,
          -0.041767f, -0.036403f, -0.017025f, -0.138155f, -0.002363f, -0.116153f}}).transpose();
  private final LayerParameter expectedConvolutionalLayerParams =
      LayerParameter.newBuilder()
          .setWeightParam(matrixFactory.create(new float[]
              {15.8f, 12.29999f, 13.9f, 9.8f}))
          .setBiasParam(matrixFactory.create(new float[]
              {2.2f}))
          .build();

  private final LayerParameter expectedConvolutionalLayerWithPaddingParams =
      LayerParameter.newBuilder()
          .setWeightParam(matrixFactory.create(new float[]
              {58.7f, 41.699997f, 58.6f, 52.300003f}))
          .setBiasParam(matrixFactory.create(new float[]
              {11.9f}))
          .build();

  private final LayerParameter expectedConvolutional3DLayerParams =
      LayerParameter.newBuilder()
          .setWeightParam(matrixFactory.create(new float[][]{
              {3, 3, 0.7f, 1, 1.1f, 0.5f, 2.2f, 1.2f},
              {2, 1, 1.2f, 0.8f, 0.1f, 0.9f, 0.3f, 1.4f}}).transpose())
          .setBiasParam(matrixFactory.create(new float[]
              {0.4f, 0.7f}))
          .build();

  private LayerBase convolutionalLayer;
  private LayerBase convolutionalWithPaddingLayer;
  private LayerBase convolutional3DLayer;

  @Before
  public void setup() {
    try {
      final Configuration layerConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(LayerIndex.class, String.valueOf(0))
          .bindNamedParameter(LayerInputShape.class, "3,3")
          .bindNamedParameter(NeuralNetworkConfigurationParameters.BatchSize.class, "2")
          .build();

      final Configuration layerConf3D = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(LayerIndex.class, String.valueOf(0))
          .bindNamedParameter(NeuralNetworkConfigurationParameters.BatchSize.class, "2")
          .bindNamedParameter(LayerInputShape.class, "2,2,3")
          .build();

      final ConvolutionalLayerConfigurationBuilder builder = ConvolutionalLayerConfigurationBuilder
          .newConfigurationBuilder()
          .setInitWeight(0.2f)
          .setInitBias(0)
          .setKernelHeight(2)
          .setKernelWidth(2)
          .setStrideHeight(1)
          .setStrideWidth(1)
          .setNumOutput(1);

      final ConvolutionalLayerConfigurationBuilder builderWithPadding = ConvolutionalLayerConfigurationBuilder
          .newConfigurationBuilder()
          .setInitWeight(0.2f)
          .setInitBias(0)
          .setPaddingHeight(1)
          .setPaddingWidth(1)
          .setKernelHeight(2)
          .setKernelWidth(2)
          .setStrideHeight(1)
          .setStrideWidth(1)
          .setNumOutput(1);

      final ConvolutionalLayerConfigurationBuilder builder3D = ConvolutionalLayerConfigurationBuilder
          .newConfigurationBuilder()
          .setInitWeight(0.2f)
          .setInitBias(0)
          .setKernelHeight(2)
          .setKernelWidth(2)
          .setStrideHeight(1)
          .setStrideWidth(1)
          .setPaddingWidth(1)
          .setNumOutput(2);

      final Injector injector = Tang.Factory.getTang().newInjector(MATRIX_CONF);

      this.convolutionalLayer = injector.forkInjector(layerConf, builder.build())
          .getInstance(LayerBase.class);

      this.convolutionalLayer.setLayerParameter(LayerParameter.newBuilder()
          .setWeightParam(matrixFactory.create(new float[]
              {-0.200013592839f, -0.095913007855f, 0.065758734941f, 0.247887045145f}))
          .setBiasParam(matrixFactory.zeros(4)).build());

      this.convolutionalWithPaddingLayer = injector.forkInjector(layerConf, builderWithPadding.build())
          .getInstance(LayerBase.class);

      this.convolutionalWithPaddingLayer.setLayerParameter(LayerParameter.newBuilder()
          .setWeightParam(matrixFactory.create(new float[]
              {-0.200013592839f, -0.095913007855f, 0.065758734941f, 0.247887045145f}))
          .setBiasParam(matrixFactory.zeros(16)).build());

      this.convolutional3DLayer = injector.forkInjector(layerConf3D, builder3D.build())
          .getInstance(LayerBase.class);

      this.convolutional3DLayer.setLayerParameter(LayerParameter.newBuilder()
          .setWeightParam(matrixFactory.create(new float[][]{
              {-0.200013592839f, 0.109642162919f},
              {-0.095913007855f, 0.021724732592f},
              {0.065758734941f, 0.138832792639f},
              {0.247887045145f, -0.207962512969f},
              {-0.129198953509f, 0.010389177128f},
              {-0.030915966257f, -0.074716188013f},
              {0.095823608338f, -0.626192688941f},
              {-0.002482861746f, -0.212796315550f}}))
          .setBiasParam(matrixFactory.zeros(8)).build());
    } catch (final InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testConvolutionalActivation() {
    final Matrix convolutionalActivation = convolutionalLayer.feedForward(input);
    assertTrue(expectedConvolutionalActivation.compare(convolutionalActivation, TOLERANCE));
  }

  @Test
  public void testConvolutionalBackPropagate() {
    final Matrix error = convolutionalLayer.backPropagate(input, expectedConvolutionalActivation, nextError);
    assertTrue(expectedConvolutionalError.compare(error, TOLERANCE));
  }

  @Test
  public void testConvolutionalWithPaddingActivation() {
    final Matrix convolutionalActivation = convolutionalWithPaddingLayer.feedForward(input);
    assertTrue(expectedConvolutionalWithPaddingActivation.compare(convolutionalActivation, TOLERANCE));
  }

  @Test
  public void testConvolutionalWithPaddingBackPropagate() {
    final Matrix error = convolutionalWithPaddingLayer.backPropagate(
        input, expectedConvolutionalWithPaddingActivation, nextErrorWithPadding);
    assertTrue(expectedConvolutionalWithPaddingError.compare(error, TOLERANCE));
  }

  @Test
  public void testConvolutionalGradient() {
    final LayerParameter convolutionalLayerParams = convolutionalLayer.generateParameterGradient(input, nextError);
    assertTrue(compare(expectedConvolutionalLayerParams, convolutionalLayerParams, TOLERANCE));
  }

  @Test
  public void testConvolutionalWithPaddingGradient() {
    final LayerParameter convolutionalLayerParams =
        convolutionalWithPaddingLayer.generateParameterGradient(input, nextErrorWithPadding);
    assertTrue(compare(expectedConvolutionalLayerWithPaddingParams, convolutionalLayerParams, TOLERANCE));
  }

  @Test
  public void testConvolutional3DActivation() {
    final Matrix convolutionalActivation = convolutional3DLayer.feedForward(input3D);
    assertTrue(expectedConvolutional3DActivation.compare(convolutionalActivation, TOLERANCE));
  }

  @Test
  public void testConvolutional3DBackPropagate() {
    final Matrix error = convolutional3DLayer.backPropagate(input3D, expectedConvolutional3DActivation, nextError3D);
    assertTrue(expectedConvolutional3DError.compare(error, TOLERANCE));
  }

  @Test
  public void testConvolutional3DGradient() {
    System.out.println("3D test begun");
    final LayerParameter convolutionalLayerParams
        = convolutional3DLayer.generateParameterGradient(input3D, nextError3D);
    assertTrue(compare(expectedConvolutional3DLayerParams, convolutionalLayerParams, TOLERANCE));
  }
}
