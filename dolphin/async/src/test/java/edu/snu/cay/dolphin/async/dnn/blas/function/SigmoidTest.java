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
package edu.snu.cay.dolphin.async.dnn.blas.function;

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.blas.jblas.MatrixJBLASFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Class for testing {@link Sigmoid}.
 */
public final class SigmoidTest {

  private static final float TOLERANCE = 1e-6f;

  private Matrix input;
  private Matrix expectedOutput;
  private Matrix expectedDerivative;

  @Before
  public void setup() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MatrixFactory.class, MatrixJBLASFactory.class)
        .build();
    final MatrixFactory matrixFactory = Tang.Factory.getTang().newInjector(conf).getInstance(MatrixFactory.class);

    final int numInput = 3;
    final int numBatch = 2;
    this.input = matrixFactory.create(new float[]{
        1.0f, 2.0f, 3.0f,
        4.0f, 5.0f, 6.0f},
        numInput, numBatch);
    this.expectedOutput = matrixFactory.create(new float[]{
        7.310585786e-01f, 8.807970780e-01f, 9.525741268e-01f,
        9.820137900e-01f, 9.933071491e-01f, 9.975273768e-01f},
        numInput, numBatch);
    this.expectedDerivative = matrixFactory.create(new float[]{
        1.966119332e-01f, 1.049935854e-01f, 4.517665973e-02f,
        1.766270621e-02f, 6.648056671e-03f, 2.466509291e-03f},
        numInput, numBatch);
  }

  @Test
  public void testSigmoidApply() {
    final Matrix output = FunctionFactory.getSingleInstance("sigmoid").apply(input);
    assertTrue(expectedOutput.compare(output, TOLERANCE));
  }

  @Test
  public void testSigmoidDerivative() {
    final Matrix derivative = FunctionFactory.getSingleInstance("sigmoid").derivative(input);
    assertTrue(expectedDerivative.compare(derivative, TOLERANCE));
  }
}
