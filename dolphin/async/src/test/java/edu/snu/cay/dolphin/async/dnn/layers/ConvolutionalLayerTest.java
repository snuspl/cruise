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
package edu.snu.cay.dolphin.async.dnn.layers;

import edu.snu.cay.dolphin.async.dnn.TestDevice;
import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixUtils;
import edu.snu.cay.dolphin.async.dnn.blas.cuda.MatrixCudaFactory;
import edu.snu.cay.dolphin.async.dnn.blas.jblas.MatrixJBLASFactory;
import edu.snu.cay.dolphin.async.dnn.conf.ConvolutionalLayerConfigurationBuilder;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.LayerIndex;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.LayerInputShape;
import edu.snu.cay.dolphin.async.dnn.conf.NeuralNetworkConfigurationParameters;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;

import static edu.snu.cay.dolphin.async.dnn.layers.LayerParameterUtils.compare;
import static org.junit.Assert.assertTrue;

/**
 * Test class for convolutional layer.
 */
@RunWith(Parameterized.class)
public class ConvolutionalLayerTest {

  private static final float TOLERANCE = 1e-4f;

  private static final float[][] INPUT = new float[][]{
      {0, 10},
      {9, 22},
      {2, 13},
      {8, 0},
      {3, 5},
      {7, 3},
      {10, 6},
      {4, 7},
      {1, 9}};
  private static final float[] WEIGHT = new float[]
      {-0.200013592839f, -0.095913007855f, 0.065758734941f, 0.247887045145f};
  private static final float[][] EXCPECTED_ACTIVATION = new float[][]{
      {0.406522f, -2.8708f},
      {-0.059460f, -4.5747f},
      {-0.238702f, 1.6502f},
      {-0.760506f, 1.4035f}};
  private static final float[][] EXCPECTED_ACTIVATION_PADDING = new float[][]{
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
      {-0.20001f, -1.8001224f}};

  private static final float[][] NEXT_ERROR = new float[][]{
      {0.1f, 0},
      {0.3f, 0.3f},
      {0.6f, 0.4f},
      {0.4f, 0.1f}};
  private static final float[][] NEXT_ERROR_PADDING = new float[][]{
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
      {0.5f, 0.3f}};
  private static final float[][] EXPECTED_ERROR = new float[][]{
      {-0.0200014f, 0},
      {-0.0695954f, -0.0600042f},
      {-0.0287739f, -0.0287739f},
      {-0.1134325f, -0.0800056f},
      {-0.0930369f, -0.0386389f},
      {0.03600091f, 0.06477481f},
      {0.0394554f, 0.0263036f},
      {0.1750358f, 0.1057307f},
      {0.0991548f, 0.0247887f}};
  private static final float[][] EXPECTED_ERROR_PADDING = new float[][]{
      {0.0369707f, -0.0337006f},
      {-0.0289132f, -0.110726f},
      {-0.0266129f, 0.0193425f},
      {-0.0471544f, -0.137827f},
      {0.0167122f, 0.0918946f},
      {0.105183f, 0.325266f},
      {0.165702f, 0.133655f},
      {-0.124812f, 0.0905167f},
      {-0.153994f, -0.0412462f}};

  private static final float[] EXPECTED_WEIGHT_GRADIENT = new float[]{15.8f, 12.29999f, 13.9f, 9.8f};
  private static final float[] EXPECTED_BIAS_GRADIENT = new float[]{2.2f};
  private static final float[] EXPECTED_WEIGHT_GRADIENT_PADDING = new float[]{58.7f, 41.699997f, 58.6f, 52.300003f};
  private static final float[] EXPECTED_BIAS_GRADIENT_PADDING = new float[]{11.9f};

  private static final float[] INPUT_3D = new float[] {0, 10, 10, 2, 0, 5, 1, 5, 0, 1, 10, 1};
  private static final float[][] WEIGHT_3D = new float[][]{
      {-0.200013592839f, 0.109642162919f},
      {-0.095913007855f, 0.021724732592f},
      {0.065758734941f, 0.138832792639f},
      {0.247887045145f, -0.207962512969f},
      {-0.129198953509f, 0.010389177128f},
      {-0.030915966257f, -0.074716188013f},
      {0.095823608338f, -0.626192688941f},
      {-0.002482861746f, -0.212796315550f}};
  private static final float[] EXPECTED_ACTIVATION_3D = new float[]
      {0.462375f, -1.040396f, -1.410072f, -1.575518f, -0.703437f, -2.622436f, -6.148920f, 1.164392f};

  private static final float[] NEXT_ERROR_3D = new float[] {0, 0.1f, 0.2f, 0.1f, 0.4f, 0.1f, 0, 0.2f};
  private static final float[] EXPECTED_ERROR_3D = new float[]
      {-0.000347f, -0.047422f, -0.017259f, -0.062726f, 0.017144f, 0.083920f,
          -0.041767f, -0.036403f, -0.017025f, -0.138155f, -0.002363f, -0.116153f};

  private static final float[][] EXPECTED_WEIGHT_GRADIENT_3D = new float[][]{
      {3, 2},
      {3, 1},
      {0.7f, 1.2f},
      {1, 0.8f},
      {1.1f, 0.1f},
      {0.5f, 0.9f},
      {2.2f, 0.3f},
      {1.2f, 1.4f}};
  private static final float[] EXPECTED_BIAS_GRADIENT_3D = new float[] {0.4f, 0.7f};

  @Parameterized.Parameters
  public static Object[] data() throws IOException {
    return TestDevice.getTestDevices();
  }

  private final boolean cpuOnly;

  private Matrix input;
  private LayerParameter parameter;
  private Matrix expectedConvolutionalActivation;
  private Matrix expectedConvolutionalWithPaddingActivation;

  private Matrix nextError;
  private Matrix nextErrorWithPadding;
  private Matrix expectedConvolutionalError;
  private Matrix expectedConvolutionalWithPaddingError;

  private LayerParameter expectedConvolutionalGradient;
  private LayerParameter expectedConvolutionalWithPaddingGradient;

  private Matrix input3D;
  private LayerParameter parameter3D;
  private Matrix expectedConvolutional3DActivation;

  private Matrix nextError3D;
  private Matrix expectedConvolutional3DError;

  private LayerParameter expectedConvolutional3DGradient;

  private LayerBase convolutionalLayer;
  private LayerBase convolutionalWithPaddingLayer;
  private LayerBase convolutional3DLayer;

  public ConvolutionalLayerTest(final String testDevice) {
    this.cpuOnly = testDevice.equals(TestDevice.CPU);
  }

  @Before
  public void setup() throws InjectionException {
    final Configuration blasConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MatrixFactory.class,
            cpuOnly ? MatrixJBLASFactory.class : MatrixCudaFactory.class)
        .build();
    final MatrixFactory matrixFactory = Tang.Factory.getTang().newInjector(blasConf).getInstance(MatrixFactory.class);
    final Injector injector = Tang.Factory.getTang().newInjector(blasConf);

    final Configuration layerConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(LayerIndex.class, String.valueOf(0))
        .bindNamedParameter(LayerInputShape.class, "1,3,3")
        .bindNamedParameter(NeuralNetworkConfigurationParameters.BatchSize.class, "2")
        .build();
    final ConvolutionalLayerConfigurationBuilder builder = ConvolutionalLayerConfigurationBuilder
        .newConfigurationBuilder()
        .setInitWeight(0.2f)
        .setInitBias(0)
        .setKernelHeight(2)
        .setKernelWidth(2)
        .setStrideHeight(1)
        .setStrideWidth(1)
        .setNumOutput(1)
        .setCpuOnly(cpuOnly);
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
        .setNumOutput(1)
        .setCpuOnly(cpuOnly);

    this.input = matrixFactory.create(INPUT);
    this.parameter = new LayerParameter(
        matrixFactory.create(WEIGHT),
        matrixFactory.zeros(1));
    this.expectedConvolutionalActivation = matrixFactory.create(EXCPECTED_ACTIVATION);
    this.expectedConvolutionalWithPaddingActivation = matrixFactory.create(EXCPECTED_ACTIVATION_PADDING);

    this.nextError = matrixFactory.create(NEXT_ERROR);
    this.nextErrorWithPadding = matrixFactory.create(NEXT_ERROR_PADDING);
    this.expectedConvolutionalError = matrixFactory.create(EXPECTED_ERROR);
    this.expectedConvolutionalWithPaddingError = matrixFactory.create(EXPECTED_ERROR_PADDING);

    this.expectedConvolutionalGradient = new LayerParameter(
        matrixFactory.create(EXPECTED_WEIGHT_GRADIENT),
        matrixFactory.create(EXPECTED_BIAS_GRADIENT));
    this.expectedConvolutionalWithPaddingGradient = new LayerParameter(
        matrixFactory.create(EXPECTED_WEIGHT_GRADIENT_PADDING),
        matrixFactory.create(EXPECTED_BIAS_GRADIENT_PADDING));

    final Configuration layerConf3D = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(LayerIndex.class, String.valueOf(0))
        .bindNamedParameter(LayerInputShape.class, "2,2,3")
        .bindNamedParameter(NeuralNetworkConfigurationParameters.BatchSize.class, "1")
        .build();
    final ConvolutionalLayerConfigurationBuilder builder3D = ConvolutionalLayerConfigurationBuilder
        .newConfigurationBuilder()
        .setInitWeight(0.2f)
        .setInitBias(0)
        .setKernelHeight(2)
        .setKernelWidth(2)
        .setStrideHeight(1)
        .setStrideWidth(1)
        .setPaddingWidth(1)
        .setNumOutput(2)
        .setCpuOnly(cpuOnly);

    this.input3D = matrixFactory.create(INPUT_3D);
    this.parameter3D = new LayerParameter(
        matrixFactory.create(WEIGHT_3D),
        matrixFactory.zeros(2));
    this.expectedConvolutional3DActivation = matrixFactory.create(EXPECTED_ACTIVATION_3D);

    this.nextError3D = matrixFactory.create(NEXT_ERROR_3D);
    this.expectedConvolutional3DError = matrixFactory.create(EXPECTED_ERROR_3D);

    this.expectedConvolutional3DGradient = new LayerParameter(
        matrixFactory.create(EXPECTED_WEIGHT_GRADIENT_3D),
        matrixFactory.create(EXPECTED_BIAS_GRADIENT_3D));

    this.convolutionalLayer = injector.forkInjector(layerConf, builder.build())
        .getInstance(LayerBase.class);
    this.convolutionalLayer.setLayerParameter(parameter);

    this.convolutionalWithPaddingLayer = injector.forkInjector(layerConf, builderWithPadding.build())
        .getInstance(LayerBase.class);
    this.convolutionalWithPaddingLayer.setLayerParameter(parameter);

    this.convolutional3DLayer = injector.forkInjector(layerConf3D, builder3D.build())
        .getInstance(LayerBase.class);
    this.convolutional3DLayer.setLayerParameter(parameter3D);
  }

  @After
  public void tearDown() {
    convolutionalLayer.cleanup();
    convolutionalWithPaddingLayer.cleanup();
    convolutional3DLayer.cleanup();

    MatrixUtils.free(input);
    MatrixUtils.free(parameter.getWeightParam());
    MatrixUtils.free(parameter.getBiasParam());
    MatrixUtils.free(expectedConvolutionalActivation);
    MatrixUtils.free(expectedConvolutionalWithPaddingActivation);

    MatrixUtils.free(nextError);
    MatrixUtils.free(nextErrorWithPadding);
    MatrixUtils.free(expectedConvolutionalError);
    MatrixUtils.free(expectedConvolutionalWithPaddingError);

    MatrixUtils.free(expectedConvolutionalGradient.getWeightParam());
    MatrixUtils.free(expectedConvolutionalGradient.getBiasParam());
    MatrixUtils.free(expectedConvolutionalWithPaddingGradient.getWeightParam());
    MatrixUtils.free(expectedConvolutionalWithPaddingGradient.getBiasParam());

    MatrixUtils.free(input3D);
    MatrixUtils.free(parameter3D.getWeightParam());
    MatrixUtils.free(parameter3D.getBiasParam());
    MatrixUtils.free(expectedConvolutional3DActivation);

    MatrixUtils.free(nextError3D);
    MatrixUtils.free(expectedConvolutional3DError);

    MatrixUtils.free(expectedConvolutional3DGradient.getWeightParam());
    MatrixUtils.free(expectedConvolutional3DGradient.getBiasParam());
  }

  @Test
  public void testConvolution() {
    final Matrix activation = convolutionalLayer.feedForward(input);
    assertTrue(expectedConvolutionalActivation.compare(activation, TOLERANCE));

    final Matrix error = convolutionalLayer.backPropagate(input, expectedConvolutionalActivation, nextError);
    assertTrue(expectedConvolutionalError.compare(error, TOLERANCE));

    final LayerParameter gradient = convolutionalLayer.generateParameterGradient(input, nextError);
    assertTrue(compare(expectedConvolutionalGradient, gradient, TOLERANCE));
  }

  @Test
  public void testConvolutionWithPadding() {
    final Matrix activation = convolutionalWithPaddingLayer.feedForward(input);
    assertTrue(expectedConvolutionalWithPaddingActivation.compare(activation, TOLERANCE));

    final Matrix error = convolutionalWithPaddingLayer.backPropagate(
        input, expectedConvolutionalWithPaddingActivation, nextErrorWithPadding);
    assertTrue(expectedConvolutionalWithPaddingError.compare(error, TOLERANCE));

    final LayerParameter gradient =
        convolutionalWithPaddingLayer.generateParameterGradient(input, nextErrorWithPadding);
    assertTrue(compare(expectedConvolutionalWithPaddingGradient, gradient, TOLERANCE));
  }

  @Test
  public void testConvolution3D() {
    final Matrix activation = convolutional3DLayer.feedForward(input3D);
    assertTrue(expectedConvolutional3DActivation.compare(activation, TOLERANCE));

    final Matrix error = convolutional3DLayer.backPropagate(input3D, expectedConvolutional3DActivation, nextError3D);
    assertTrue(expectedConvolutional3DError.compare(error, TOLERANCE));

    final LayerParameter gradient
        = convolutional3DLayer.generateParameterGradient(input3D, nextError3D);
    assertTrue(compare(expectedConvolutional3DGradient, gradient, TOLERANCE));
  }
}
