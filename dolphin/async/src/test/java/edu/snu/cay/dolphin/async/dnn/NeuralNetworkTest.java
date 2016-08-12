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
      {-1.0000679321819916e-4f, -1.54579829541035e-5f, 6.941639730939642e-5f, -1.0639814718160778e-4f,
          2.24940842599608e-4f, 2.4672370636835694e-4f, -1.0656801168806851e-4f, -4.036870814161375e-5f},
      {-4.795650238520466e-5f, 4.791180253960192e-5f, -1.039812559611164e-4f, -5.148160198586993e-5f,
          1.5828292816877365e-4f, 5.05058269482106e-5f, 6.632897566305473e-5f, 5.640013114316389e-5f},
      {3.287936488050036e-5f, -1.2414308230290771e-6f, 5.194588084123097e-6f, -4.8094203521031886e-5f,
          7.82513598096557e-5f, 3.6573746911017224e-5f, -1.1968148464802653e-4f, 1.214973526657559e-4f},
      {1.2394352233968675e-4f, 5.482107735588215e-5f, -3.735809150384739e-5f, -2.8865197236882523e-5f,
          1.0060314525617287e-4f, 1.370291556668235e-5f, 1.0453909635543823e-4f, 9.480212611379102e-6f},
      {-6.459947326220572e-5f, 1.086236625269521e-5f, -3.1309633050113916e-4f, -4.435202936292626e-5f,
          4.678771074395627e-5f, -1.4785238818149082e-5f, 2.2910378902452067e-5f, 4.293724850867875e-5f}});



  private final Matrix biasOne = matrixFactory.create(new float[]
      {1.9999999494757503e-4f, 1.9999999494757503e-4f, 1.9999999494757503e-4f,
          1.9999999494757503e-4f, 1.9999999494757503e-4f});

  private final Matrix weightTwo = matrixFactory.create(new float[][]{
      {0.20476184785366058f, 0.12248460203409195f, 0.22612512111663818f, 0.35694047808647156f, 0.1705240160226822f},
      {0.07434597611427307f, -0.1554500311613083f, 0.10352678596973419f, 0.0948064848780632f, -0.142625629901886f},
      {0.052237618714571f, 0.007420114707201719f, -0.2848993241786957f, 0.0036804750561714172f, 0.26537343859672546f}});

  private final Matrix biasTwo = matrixFactory.create(new float[]
      {0.30000001192092896f, 0.30000001192092896f, 0.30000001192092896f});

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
      .addLayerConfiguration(
          FullyConnectedLayerConfigurationBuilder.newConfigurationBuilder()
              .setNumOutput(numHiddenUnits)
              .setInitWeight(0.0001f)
              .setInitBias(0.0002f)
              .build())
      .addLayerConfiguration(
          ActivationLayerConfigurationBuilder.newConfigurationBuilder()
              .setActivationFunction("sigmoid")
              .build())
      .addLayerConfiguration(
          FullyConnectedLayerConfigurationBuilder.newConfigurationBuilder()
              .setNumOutput(expectedOutput.getLength())
              .setInitWeight(0.2f)
              .setInitBias(0.3f)
              .build())
      .addLayerConfiguration(ActivationWithLossLayerConfigurationBuilder.newConfigurationBuilder()
          .setActivationFunction("sigmoid")
          .setLossFunction("crossentropy")
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
        final LayerParameter layerParameterOne = LayerParameter.newBuilder()
            .setWeightParam(weightOne)
            .setBiasParam(biasOne)
            .build();
        final LayerParameter layerParameterTwo = LayerParameter.newBuilder()
            .setWeightParam(weightTwo)
            .setBiasParam(biasTwo)
            .build();
        final List<LayerParameter> result = new ArrayList<>();
        result.add(layerParameterOne);
        result.add(layerParameterTwo);
        return result;
      }).when(mockParameterWorker).pull(anyObject());

    neuralNetwork.updateParameters();
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
