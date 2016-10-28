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
import edu.snu.cay.dolphin.async.dnn.blas.cuda.MatrixCudaFactory;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for neural network.
 */
@RunWith(Parameterized.class)
public final class NeuralNetworkTest {

  private static final float TOLERANCE = 1e-7f;

  private static final int NUM_HIDDEN_UNITS = 5;
  private static final int BATCH_SIZE = 3;

  @Parameterized.Parameters
  public static Object[] data() throws IOException {
    return TestDevice.getTestDevices();
  }

  private final boolean cpuOnly;

  private Matrix weightOne;

  private Matrix biasOne;

  private Matrix weightTwo;

  private Matrix biasTwo;

  private Matrix input;

  private Matrix expectedOutput;

  private Matrix label;

  private Matrix[] expectedActivations;

  private Matrix[] expectedErrors;

  private Matrix batchInput;

  private Matrix expectedBatchOutput;

  private Matrix labels;

  private Matrix[] expectedBatchActivations;

  private Matrix[] expectedBatchErrors;

  private NeuralNetwork neuralNetwork;

  public NeuralNetworkTest(final String testDevice) throws InjectionException {
    cpuOnly = testDevice.equals(TestDevice.CPU);
  }

  @Before
  public void buildNeuralNetwork() throws InjectionException {
    final Configuration blasConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MatrixFactory.class,
            cpuOnly ? MatrixJBLASFactory.class : MatrixCudaFactory.class)
        .build();
    final MatrixFactory matrixFactory = Tang.Factory.getTang().newInjector(blasConf).getInstance(MatrixFactory.class);

    this.weightOne = matrixFactory.create(new float[][]{
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

    this.biasOne = matrixFactory.create(new float[]
        {1.99999994947e-04f, 1.99999994947e-04f, 1.99999994947e-04f, 1.99999994947e-04f, 1.99999994947e-04f});

    this.weightTwo = matrixFactory.create(new float[][]{
        {0.20476184785f, 0.12248460203f, 0.22612512111f, 0.35694047808f, 0.17052401602f},
        {0.07434597611f, -0.15545003116f, 0.10352678596f, 0.09480648487f, -0.14262562990f},
        {0.05223761871f, 0.00742011470f, -0.28489932417f, 0.00368047505f, 0.26537343859f}});

    this.biasTwo = matrixFactory.create(new float[]
        {0.30000001192f, 0.30000001192f, 0.30000001192f});

    this.input = matrixFactory.create(new float[]{77, 57, 30, 26, 75, 74, 87, 75});
    this.expectedOutput =
        matrixFactory.create(new float[]{0.46177076f, 0.26465988f, 0.27356936f});
    this.label = matrixFactory.create(new float[]{0, 1, 0});

    this.expectedActivations = new Matrix[]{
        matrixFactory.create(new float[]{
            1.37635586396e-02f, 2.03896453321e-02f, 8.84165966865e-03f, 2.93623840734e-02f, -7.07257980630e-03f}),
        matrixFactory.create(new float[]{
            5.03440835342e-01f, 5.05097234742e-01f, 5.02210400517e-01f, 5.07340068673e-01f, 4.98231862419e-01f}),
        matrixFactory.create(new float[]{
            8.44565190562e-01f, 2.87942143690e-01f, 3.21051780914e-01f}),
        expectedOutput};

    this.expectedErrors = new Matrix[]{
        matrixFactory.create(new float[]{
            0.01354287658f, 0.04322009906f, -0.01241204422f, 0.02402395569f, 0.06405404210f}),
        matrixFactory.create(new float[]{
            0.05417407304f, 0.17289836705f, -0.04964914918f, 0.09611653537f, 0.25621938705f}),
        matrixFactory.create(new float[]{0.46177077293f, -0.73534011841f, 0.27356934547f})};

    final Configuration neuralNetworkConfiguration = NeuralNetworkConfigurationBuilder.newConfigurationBuilder()
        .setInputShape(input.getLength(), 1, 1)
        .setStepSize(1e-2f)
        .setRandomSeed(10)
        .setCpuOnly(cpuOnly)
        .addLayerConfiguration(FullyConnectedLayerConfigurationBuilder.newConfigurationBuilder()
            .setNumOutput(NUM_HIDDEN_UNITS)
            .setInitWeight(0.0001f)
            .setInitBias(0.0002f)
            .setCpuOnly(cpuOnly)
            .build())
        .addLayerConfiguration(ActivationLayerConfigurationBuilder.newConfigurationBuilder()
            .setActivationFunction("sigmoid")
            .setCpuOnly(cpuOnly)
            .build())
        .addLayerConfiguration(FullyConnectedLayerConfigurationBuilder.newConfigurationBuilder()
            .setNumOutput(expectedOutput.getLength())
            .setInitWeight(0.2f)
            .setInitBias(0.3f)
            .setCpuOnly(cpuOnly)
            .build())
        .addLayerConfiguration(ActivationWithLossLayerConfigurationBuilder.newConfigurationBuilder()
            .setActivationFunction("softmax")
            .setLossFunction("crossentropy")
            .setCpuOnly(cpuOnly)
            .build())
        .build();

    this.batchInput = matrixFactory.create(new float[]{
        77, 57, 30, 26, 75, 74, 87, 75,
        61, 5, 18, 18, 16, 4, 67, 29,
        68, 85, 4, 50, 19, 3, 5, 18}, input.getLength(), BATCH_SIZE);

    this.expectedBatchOutput = matrixFactory.create(new float[]{
        4.61770762870e-01f, 2.64659881819e-01f, 2.73569355310e-01f,
        4.60887641401e-01f, 2.64983657349e-01f, 2.74128701250e-01f,
        4.60964312255e-01f, 2.64988135049e-01f, 2.74047552696e-01f}, expectedOutput.getLength(), BATCH_SIZE);

    this.labels = matrixFactory.create(new float[]{
        0, 1, 0,
        0, 0, 1,
        1, 0, 0}, expectedOutput.getLength(), BATCH_SIZE);

    this.expectedBatchActivations = new Matrix[]{
        matrixFactory.create(new float[]{
            1.37635586396e-02f, 2.03896453321e-02f, 8.84165966865e-03f, 2.93623840734e-02f, -7.07257980630e-03f,
            -1.03681771740e-02f, 3.53007655740e-03f, -1.66967848856e-03f, 1.57861485705e-02f, -6.65068837926e-03f,
            -9.20206232816e-03f, 2.52719653249e-03f, 3.13138561007e-03f, 1.43411668226e-02f, -5.00741667077e-03f},
            NUM_HIDDEN_UNITS, BATCH_SIZE),
        matrixFactory.create(new float[]{
            5.03440835342e-01f, 5.05097234742e-01f, 5.02210400517e-01f, 5.07340068673e-01f, 4.98231862419e-01f,
            4.97407978926e-01f, 5.00882518223e-01f, 4.99582580475e-01f, 5.03946455187e-01f, 4.98337334034e-01f,
            4.97699500651e-01f, 5.00631798797e-01f, 5.00782845763e-01f, 5.03585230258e-01f, 4.98748148448e-01f},
            NUM_HIDDEN_UNITS, BATCH_SIZE),
        matrixFactory.create(new float[]{
            8.44565190562e-01f, 2.87942143690e-01f, 3.21051780914e-01f,
            8.41026105227e-01f, 2.87539973621e-01f, 3.21469528616e-01f,
            8.41267616553e-01f, 2.87632041900e-01f, 3.21248631631e-01f},
            expectedOutput.getLength(), BATCH_SIZE),
        expectedBatchOutput};

    this.expectedBatchErrors = new Matrix[]{
        matrixFactory.create(new float[]{
            1.35428765789e-02f, 4.32200990617e-02f, -1.24120442197e-02f, 2.40239556879e-02f, 6.40540421009e-02f,
            1.90382115543e-02f, 2.46846140362e-03f, 8.46128016710e-02f, 4.67371083796e-02f, -3.79565842450e-02f,
            -1.90889853984e-02f, -2.62955874205e-02f, -4.31329198182e-02f, -4.15659695864e-02f, -1.42468335107e-02f},
            NUM_HIDDEN_UNITS, BATCH_SIZE),
        matrixFactory.create(new float[]{
            5.41740730405e-02f, 1.72898367047e-01f, -4.96491491795e-02f, 9.61165353656e-02f, 2.56219387054e-01f,
            7.61548876762e-02f, 9.87387634814e-03f, 3.38451445103e-01f, 1.86960071325e-01f, -1.51828020811e-01f,
            -7.63575509191e-02f, -1.05182521045e-01f, -1.72532096505e-01f, -1.66272431612e-01f, -5.69876879454e-02f},
            NUM_HIDDEN_UNITS, BATCH_SIZE),
        matrixFactory.create(new float[]{
            4.61770772934e-01f, -7.35340118408e-01f, 2.73569345474e-01f,
            4.60887640715e-01f, 2.64983654022e-01f, -7.25871324539e-01f,
            -5.39035677910e-01f, 2.64988124371e-01f, 2.74047553539e-01f},
            expectedOutput.getLength(), BATCH_SIZE)};

    final Injector injector = Tang.Factory.getTang().newInjector(blasConf, neuralNetworkConfiguration);
    final ParameterWorker mockParameterWorker = mock(ParameterWorker.class);
    injector.bindVolatileInstance(ParameterWorker.class, mockParameterWorker);
    this.neuralNetwork = injector.getInstance(NeuralNetwork.class);

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

    MatrixUtils.free(weightOne);
    MatrixUtils.free(biasOne);
    MatrixUtils.free(weightTwo);
    MatrixUtils.free(biasTwo);
    MatrixUtils.free(input);
    MatrixUtils.free(expectedOutput);
    MatrixUtils.free(label);
    for (final Matrix m : expectedActivations) {
      MatrixUtils.free(m);
    }
    for (final Matrix m : expectedErrors) {
      MatrixUtils.free(m);
    }
    MatrixUtils.free(batchInput);
    MatrixUtils.free(expectedBatchOutput);
    MatrixUtils.free(labels);
    for (final Matrix m : expectedBatchActivations) {
      MatrixUtils.free(m);
    }
    for (final Matrix m : expectedBatchErrors) {
      MatrixUtils.free(m);
    }
  }

  /**
   * Unit test for neural network.
   */
  @Test
  public void neuralNetworkTest() {
    final Matrix[] activations = neuralNetwork.feedForward(input);
    assertTrue(MatrixUtils.compare(activations, expectedActivations, TOLERANCE));
    final Matrix[] errors = neuralNetwork.backPropagate(ArrayUtils.add(activations, 0, input), label);
    assertTrue(MatrixUtils.compare(errors, expectedErrors, TOLERANCE));
  }

  /**
   * Unit test for neural network with a batch input.
   */
  @Test
  public void neuralNetworkTestForBatch() {
    final Matrix[] batchActivations = neuralNetwork.feedForward(batchInput);
    assertTrue(MatrixUtils.compare(batchActivations, expectedBatchActivations, TOLERANCE));
    final Matrix[] batchErrors = neuralNetwork.backPropagate(ArrayUtils.add(batchActivations, 0, batchInput), labels);
    assertTrue(MatrixUtils.compare(batchErrors, expectedBatchErrors, TOLERANCE));
  }
}
