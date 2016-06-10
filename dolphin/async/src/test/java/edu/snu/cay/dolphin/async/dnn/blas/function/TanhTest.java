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
 * Class for testing {@link Tanh}.
 */
public final class TanhTest {

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
        7.615941560e-01f, 9.640275801e-01f, 9.950547537e-01f,
        9.993292997e-01f, 9.999092043e-01f, 9.999877117e-01f},
        numInput, numBatch);
    this.expectedDerivative = matrixFactory.create(new float[]{
        4.199743416e-01f, 7.065082485e-02f, 9.866037165e-03f,
        1.340950683e-03f, 1.815832309e-04f, 2.457654741e-05f},
        numInput, numBatch);
  }

  @Test
  public void testTanhApply() {
    final Matrix output = FunctionFactory.getSingleInstance("tanh").apply(input);
    assertTrue(expectedOutput.compare(output, TOLERANCE));
  }

  @Test
  public void testTanhDerivative() {
    final Matrix derivative = FunctionFactory.getSingleInstance("tanh").derivative(input);
    assertTrue(expectedDerivative.compare(derivative, TOLERANCE));
  }
}
