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
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
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

  private final Matrix input = matrixFactory.create(new float[]{77, 57, 30, 26, 75, 74, 87, 75});
  private final Matrix expectedOutput =
      matrixFactory.create(new float[]{5.96001906923e-01f, 5.54388444438e-01f, 4.88766051614e-01f});
  private final Matrix label = matrixFactory.create(new float[]{0, 1, 0});
  private final int numHiddenUnits = 5;

  private final Matrix[] expectedActivations = new Matrix[] {
      matrixFactory.create(new float[]{
          1.37635586396e-02f, 2.03896453321e-02f, 8.84165966865e-03f, 2.93623840734e-02f, -7.07257980630e-03f}),
      matrixFactory.create(new float[]{
          5.03440835342e-01f, 5.05097234742e-01f, 5.02210400517e-01f, 5.07340068673e-01f, 4.98231862419e-01f}),
      matrixFactory.create(new float[]{
          3.88833708754e-01f, 2.18417983427e-01f, -4.49433571251e-02f}),
      expectedOutput};

  private final Configuration neuralNetworkConfiguration = NeuralNetworkConfigurationBuilder.newConfigurationBuilder()
      .setInputShape(input.getLength())
      .setStepSize(1e-2f)
      .addLayerConfiguration(
          FullyConnectedLayerConfigurationBuilder.newConfigurationBuilder()
              .setNumOutput(numHiddenUnits)
              .setInitWeight(0.0001f)
              .setInitBias(0.0002f)
              .setRandomSeed(10)
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
              .setRandomSeed(10)
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

  private final Matrix[] expectedErrors = new Matrix[] {
      matrixFactory.create(new float[]{
          -1.10814502938e-02f, 4.75458121280e-02f, 2.79511566348e-02f, -3.76325213356e-02f, -6.66430044053e-02f}),
      matrixFactory.create(new float[]{
          -4.43279004290e-02f, 1.90203015780e-01f, 1.11806811634e-01f, -1.50562532537e-01f, -2.66575351211e-01f}),
      matrixFactory.create(new float[]{5.96001906923e-01f, -4.45611555562e-01f, 4.88766051614e-01f})};

  @Before
  public void buildNeuralNetwork() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(blasConfiguration, neuralNetworkConfiguration);
    injector.bindVolatileInstance(ParameterWorker.class, mock(ParameterWorker.class));
    neuralNetwork = injector.getInstance(NeuralNetwork.class);
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
      5.96001906923e-01f, 5.54388444438e-01f, 4.88766051614e-01f,
      5.95962765220e-01f, 5.54549180394e-01f, 4.88787332887e-01f,
      5.95960592285e-01f, 5.54529568429e-01f, 4.88781434177e-01f}, expectedOutput.getLength(), numBatch);

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
          3.88833708754e-01f, 2.18417983427e-01f, -4.49433571251e-02f,
          3.88671151640e-01f, 2.19068648969e-01f, -4.48581891244e-02f,
          3.88662127499e-01f, 2.18989256483e-01f, -4.48817958414e-02f},
          expectedOutput.getLength(), numBatch),
      expectedBatchOutput};

  private final Matrix[] expectedBatchErrors = new Matrix[] {
      matrixFactory.create(new float[]{
          -1.10814502938e-02f, 4.75458121280e-02f, 2.79511566348e-02f, -3.76325213356e-02f, -6.66430044053e-02f,
          -5.15000730878e-02f, 2.29721560018e-02f, -8.00065487467e-05f, 4.90593973619e-02f, 7.12180587144e-02f,
          1.49417896767e-02f, -4.67279048839e-02f, 3.37442108292e-03f, -8.35931344072e-03f, -8.79247789398e-02f},
          numHiddenUnits, numBatch),
      matrixFactory.create(new float[]{
          -4.43279004290e-02f, 1.90203015780e-01f, 1.11806811634e-01f, -1.50562532537e-01f, -2.66575351211e-01f,
          -2.06005828612e-01f, 9.18889102737e-02f, -3.20026418031e-04f, 1.96249815425e-01f, 2.84875384962e-01f,
          5.97684239558e-02f, -1.86911917974e-01f, 1.34977174198e-02f, -3.34389730445e-02f, -3.51701320409e-01f},
          numHiddenUnits, numBatch),
      matrixFactory.create(new float[]{
          5.96001906923e-01f, -4.45611555562e-01f, 4.88766051614e-01f,
          5.95962765220e-01f, 5.54549180394e-01f, -5.11212667113e-01f,
          -4.04039407715e-01f, 5.54529568429e-01f, 4.88781434177e-01f},
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
