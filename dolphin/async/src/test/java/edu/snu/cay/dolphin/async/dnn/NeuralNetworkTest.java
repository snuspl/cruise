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
import edu.snu.cay.dolphin.async.dnn.blas.MatrixUtils;
import edu.snu.cay.dolphin.async.dnn.blas.jblas.MatrixJBLASFactory;
import edu.snu.cay.dolphin.async.dnn.conf.ActivationLayerConfigurationBuilder;
import edu.snu.cay.dolphin.async.dnn.conf.ActivationWithLossLayerConfigurationBuilder;
import edu.snu.cay.dolphin.async.dnn.conf.FullyConnectedLayerConfigurationBuilder;
import edu.snu.cay.dolphin.async.dnn.conf.NeuralNetworkConfigurationBuilder;
import edu.snu.cay.dolphin.async.dnn.layers.LayerParameter;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for neural network.
 */
public final class NeuralNetworkTest {

  private static MatrixFactory matrixFactory;
  private static final float TOLERANCE = 1e-7f;

  static {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MatrixFactory.class, MatrixJBLASFactory.class)
        .build();
    try {
      matrixFactory = Tang.Factory.getTang().newInjector(configuration).getInstance(MatrixFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("InjectionException while injecting a matrix factory: " + e);
    }
  }

  private final Matrix weightOne = matrixFactory.create(new float[][] {
      {-1.00006793218e-04f, -1.54579829541e-05f, 6.94163973093e-05f, -1.06398147181e-04f,
          2.24940842599e-04f, 2.46723706368e-04f, -1.06568011688e-04f, -4.03687081416e-05f},
      {-4.79565023852e-05f, 4.79118025396e-05f, -1.03981255961e-04f, -5.14816019858e-05f,
          1.58282928168e-04f, 5.05058269482e-05f, 6.63289756630e-05f, 5.64001311431e-05f},
      {3.28793648805e-05f, -1.24143082302e-06f, 5.19458808412e-06f, -4.80942035210e-05f,
          7.82513598096e-05f, 3.65737469110e-05f, -1.19681484648e-04f, 1.21497352665e-04f},
      {1.23943522339e-04f, 5.48210773558e-05f, -3.73580915038e-05f, -2.88651972368e-05f,
          1.00603145256e-04f, 1.37029155666e-05f, 1.04539096355e-04f, 9.48021261137e-06f},
      {-6.45994732622e-05f, 1.08623662526e-05f, -3.13096330501e-04f, -4.43520293629e-05f,
          4.67877107439e-05f, -1.47852388181e-05f, 2.29103789024e-05f, 4.29372485086e-05f}});

  private final Matrix biasOne = matrixFactory.create(new float[]
      {1.99999994947e-04f, 1.99999994947e-04f, 1.99999994947e-04f, 1.99999994947e-04f, 1.99999994947e-04f});

  private final Matrix weightTwo = matrixFactory.create(new float[][]{
      {0.20476184785f, 0.12248460203f, 0.22612512111f, 0.35694047808f, 0.17052401602f},
      {0.07434597611f, -0.15545003116f, 0.10352678596f, 0.09480648487f, -0.14262562990f},
      {0.05223761871f, 0.00742011470f, -0.28489932417f, 0.00368047505f, 0.26537343859f}});

  private final Matrix biasTwo = matrixFactory.create(new float[]
      {0.30000001192f, 0.30000001192f, 0.30000001192f});

  private final Matrix input = matrixFactory.create(new float[]{77, 57, 30, 26, 75, 74, 87, 75});
  private final Matrix expectedOutput =
      matrixFactory.create(new float[]{6.99425825888e-01f, 5.71492261161e-01f, 5.79580557810e-01f});
  private final Matrix label = matrixFactory.create(new float[]{0, 1, 0});
  private final int numHiddenUnits = 5;

  private final Matrix[] expectedActivations = new Matrix[] {
      matrixFactory.create(new float[]{
          1.37635586396e-02f, 2.03896453321e-02f, 8.84165966865e-03f, 2.93623840734e-02f, -7.07257980630e-03f}),
      matrixFactory.create(new float[]{
          5.03440835342e-01f, 5.05097234742e-01f, 5.02210400517e-01f, 5.07340068673e-01f, 4.98231862419e-01f}),
      matrixFactory.create(new float[]{
          8.44565190562e-01f, 2.87942143690e-01f, 3.21051780914e-01f}),
      expectedOutput};

  private final Configuration neuralNetworkConfiguration = NeuralNetworkConfigurationBuilder.newConfigurationBuilder()
      .setInputShape(input.getLength())
      .setStepSize(1e-2f)
      .setRandomSeed(10)
      .addLayerConfiguration(FullyConnectedLayerConfigurationBuilder.newConfigurationBuilder()
          .setNumOutput(numHiddenUnits)
          .setInitWeight(0.0001f)
          .setInitBias(0.0002f)
          .setCpuOnly(true)
          .build())
      .addLayerConfiguration(ActivationLayerConfigurationBuilder.newConfigurationBuilder()
          .setActivationFunction("sigmoid")
          .setCpuOnly(true)
          .build())
      .addLayerConfiguration(FullyConnectedLayerConfigurationBuilder.newConfigurationBuilder()
          .setNumOutput(expectedOutput.getLength())
          .setInitWeight(0.2f)
          .setInitBias(0.3f)
          .setCpuOnly(true)
          .build())
      .addLayerConfiguration(ActivationWithLossLayerConfigurationBuilder.newConfigurationBuilder()
          .setActivationFunction("sigmoid")
          .setLossFunction("crossentropy")
          .setCpuOnly(true)
          .build())
      .build();

  private final Configuration blasConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
      .bindImplementation(MatrixFactory.class, MatrixJBLASFactory.class)
      .build();

  private NeuralNetwork neuralNetwork;
  private ParameterWorker mockParameterWorker;

  private final Matrix[] expectedErrors = new Matrix[] {
      matrixFactory.create(new float[]{
          3.54067743975e-02f, 3.91411779548e-02f, -1.28313456911e-02f, 5.27789222833e-02f, 8.35465482582e-02f}),
      matrixFactory.create(new float[]{
          1.41633804997e-01f, 1.56580984844e-01f, -5.13263858606e-02f, 2.11161195729e-01f, 3.34190372164e-01f}),
      matrixFactory.create(new float[]{6.99425825888e-01f, -4.28507738839e-01f, 5.79580557810e-01f})};

  @Before
  public void buildNeuralNetwork() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(blasConfiguration, neuralNetworkConfiguration);
    mockParameterWorker = mock(ParameterWorker.class);
    injector.bindVolatileInstance(ParameterWorker.class, mockParameterWorker);
    neuralNetwork = injector.getInstance(NeuralNetwork.class);

    doAnswer(invocation -> {
        final LayerParameter layerParameterOne = new LayerParameter(weightOne, biasOne);
        final LayerParameter layerParameterTwo = new LayerParameter(weightTwo, biasTwo);
        final List<LayerParameter> result = new ArrayList<>();
        result.add(layerParameterOne);
        result.add(layerParameterTwo);
        return result;
      }).when(mockParameterWorker).pull(anyObject());

    neuralNetwork.updateParameters();
  }

  @After
  public void tearDown() {
    neuralNetwork.cleanup();
  }

  /**
   * Unit test for feedforward of neural network.
   */
  @Test
  public void feedForwardTest() {
    final Matrix[] activations = neuralNetwork.feedForward(input);
    assertTrue(expectedOutput.compare(activations[activations.length - 1], TOLERANCE));
    assertTrue(MatrixUtils.compare(activations, expectedActivations, TOLERANCE));
  }

  /**
   * Unit test for backprogation of neural network.
   */
  @Test
  public void backPropagateTest() {
    final Matrix[] activations = neuralNetwork.feedForward(input);
    assertTrue(MatrixUtils.compare(activations, expectedActivations, TOLERANCE));

    final Matrix[] gradients = neuralNetwork.backPropagate(ArrayUtils.add(activations, 0, input), label);
    assertTrue(MatrixUtils.compare(expectedErrors, gradients, TOLERANCE));
  }

  private final int numBatch = 3;

  private final Matrix batchInput = matrixFactory.create(new float[]{
      77, 57, 30, 26, 75, 74, 87, 75,
      61, 5, 18, 18, 16, 4, 67, 29,
      68, 85, 4, 50, 19, 3, 5, 18}, input.getLength(), numBatch);

  private final Matrix expectedBatchOutput = matrixFactory.create(new float[]{
      6.99425825888e-01f, 5.71492261161e-01f, 5.79580557810e-01f,
      6.98681281603e-01f, 5.71393771362e-01f, 5.79682345726e-01f,
      6.98732123516e-01f, 5.71416319005e-01f, 5.79628523069e-01f}, expectedOutput.getLength(), numBatch);

  private final Matrix labels = matrixFactory.create(new float[]{
      0, 1, 0,
      0, 0, 1,
      1, 0, 0}, expectedOutput.getLength(), numBatch);

  private final Matrix[] expectedBatchActivations = new Matrix[] {
      matrixFactory.create(new float[]{
          1.37635586396e-02f, 2.03896453321e-02f, 8.84165966865e-03f, 2.93623840734e-02f, -7.07257980630e-03f,
          -1.03681771740e-02f, 3.53007655740e-03f, -1.66967848856e-03f, 1.57861485705e-02f, -6.65068837926e-03f,
          -9.20206232816e-03f, 2.52719653249e-03f, 3.13138561007e-03f, 1.43411668226e-02f, -5.00741667077e-03f},
          numHiddenUnits, numBatch),
      matrixFactory.create(new float[]{
          5.03440835342e-01f, 5.05097234742e-01f, 5.02210400517e-01f, 5.07340068673e-01f, 4.98231862419e-01f,
          4.97407978926e-01f, 5.00882518223e-01f, 4.99582580475e-01f, 5.03946455187e-01f, 4.98337334034e-01f,
          4.97699500651e-01f, 5.00631798797e-01f, 5.00782845763e-01f, 5.03585230258e-01f, 4.98748148448e-01f},
          numHiddenUnits, numBatch),
      matrixFactory.create(new float[]{
          8.44565190562e-01f, 2.87942143690e-01f, 3.21051780914e-01f,
          8.41026105227e-01f, 2.87539973621e-01f, 3.21469528616e-01f,
          8.41267616553e-01f, 2.87632041900e-01f, 3.21248631631e-01f},
          expectedOutput.getLength(), numBatch),
      expectedBatchOutput};

  private final Matrix[] expectedBatchErrors = new Matrix[] {
      matrixFactory.create(new float[]{
          3.54067743975e-02f, 3.91411779548e-02f, -1.28313456911e-02f, 5.27789222833e-02f, 8.35465482582e-02f,
          4.08958273398e-02f, -1.59106512616e-03f, 8.42229824251e-02f, 7.54984157076e-02f, -1.84734459806e-02f,
          2.76812889594e-03f, -3.03565626334e-02f, -4.35256740938e-02f, -1.28061956655e-02f, 5.23646752384e-03f},
          numHiddenUnits, numBatch),
      matrixFactory.create(new float[]{
          1.41633804997e-01f, 1.56580984844e-01f, -5.13263858606e-02f, 2.11161195729e-01f, 3.34190372164e-01f,
          1.63587705663e-01f, -6.36428033164e-03f, 3.36892164500e-01f, 3.02012477614e-01f, -7.38946010364e-02f,
          1.10727499849e-02f, -1.21426444413e-01f, -1.74103123170e-01f, -5.12274165454e-02f, 2.09460013960e-02f},
          numHiddenUnits, numBatch),
      matrixFactory.create(new float[]{
          6.99425825888e-01f, -4.28507738839e-01f, 5.79580557810e-01f,
          6.98681281603e-01f, 5.71393771362e-01f, -4.20317654274e-01f,
          -3.01267876484e-01f, 5.71416319005e-01f, 5.79628523069e-01f},
          expectedOutput.getLength(), numBatch)};

  /**
   * Unit test for feedforward of neural network for a batch input.
   */
  @Test
  public void feedForwardTestForBatch() {
    final Matrix[] batchActivations = neuralNetwork.feedForward(batchInput);
    assertTrue(expectedBatchOutput.compare(batchActivations[batchActivations.length - 1], TOLERANCE));
    assertTrue(MatrixUtils.compare(batchActivations, expectedBatchActivations, TOLERANCE));
  }

  /**
   * Unit test for backpropagate of neural network for a batch input.
   */
  @Test
  public void backPropagateTestForBatch() {
    final Matrix[] batchActivations = ArrayUtils.add(expectedBatchActivations, 0, batchInput);
    final Matrix[] gradients = neuralNetwork.backPropagate(batchActivations, labels);
    assertTrue(MatrixUtils.compare(expectedBatchErrors, gradients, TOLERANCE));
  }
}
