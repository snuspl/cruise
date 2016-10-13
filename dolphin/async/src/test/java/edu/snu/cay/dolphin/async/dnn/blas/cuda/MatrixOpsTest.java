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
package edu.snu.cay.dolphin.async.dnn.blas.cuda;

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixUtils;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static edu.snu.cay.dolphin.async.dnn.blas.cuda.FloatCompare.assertArrayEquals;
import static edu.snu.cay.dolphin.async.dnn.blas.cuda.FloatCompare.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link MatrixCudaImpl} operations.
 */
public final class MatrixOpsTest {
  private MatrixFactory matrixFactory;

  @Before
  public void setUp() {
    try {
      matrixFactory = Tang.Factory.getTang().newInjector().getInstance(MatrixCudaFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Tests basic matrix operations.
   */
  @Test
  public void testBasicOps() {
    final float[] input = {0.1F, 0.2F, 0.3F, 0.4F, 0.5F, 0.6F};
    final Matrix m = matrixFactory.create(input, 3, 2);
    assertEquals(0.5F, m.get(4));
    assertEquals(0.3F, m.get(2, 0));
    m.put(4, 5.0F);
    m.put(0, 1, -4.0F);
    assertEquals(-4.0F, m.get(3));
    assertEquals(5.0F, m.get(1, 1));
    assertEquals(-4.0F, m.min());
    assertEquals(5.0F, m.max());
    assertEquals(2.2F, m.sum());

    final Matrix dup = m.dup();
    dup.divi(3.0F);
    dup.muli(3.0F);
    assertTrue(m.compare(dup, 2.0E-7F));

    final Matrix m1 = m.columnSums();
    assertArrayEquals(new float[]{0.6F, 1.6F}, m1.toFloatArray());
    MatrixUtils.free(m1);
    final Matrix columnMaxs = m.columnMaxs();
    assertArrayEquals(new float[]{0.3F, 5.0F}, columnMaxs.toFloatArray());
    MatrixUtils.free(columnMaxs);
    final Matrix columnMins = m.columnMins();
    assertArrayEquals(new float[]{0.1F, -4.0F}, columnMins.toFloatArray());
    MatrixUtils.free(columnMins);

    final Matrix m2 = m.rowSums();
    assertArrayEquals(new float[]{-3.9F, 5.2F, 0.9F}, m2.toFloatArray());
    MatrixUtils.free(m2);

    final Matrix m3 = matrixFactory.create(m.getRows(), 1);
    m.rowSums(m3);
    MatrixUtils.free(m3);

    final Matrix rowMaxs = m.rowMaxs();
    assertArrayEquals(new float[]{0.1F, 5.0F, 0.6F}, rowMaxs.toFloatArray());
    MatrixUtils.free(rowMaxs);
    final Matrix rowMins = m.rowMins();
    assertArrayEquals(new float[]{-4.0F, 0.2F, 0.3F}, rowMins.toFloatArray());
    MatrixUtils.free(rowMins);

    MatrixUtils.free(m);
  }

  /**
   * Tests matrix-scalar operations.
   */
  @Test
  public void testScalarOps() {
    final float[] input = {0.1F, 0.2F, 0.3F, 0.4F, 0.5F, 0.6F};
    final float[] output1 = {1.1F, 1.2F, 1.3F, 1.4F, 1.5F, 1.6F};
    final float[] output2 = {0.2F, 0.4F, 0.6F, 0.8F, 1.0F, 1.2F};
    final float[] output3 = {0.9F, 0.8F, 0.7F, 0.6F, 0.5F, 0.4F};
    final float[] output4 = {6.0F, 3.0F, 2.0F, 1.5F, 1.2F, 1.0F};
    final Matrix m = matrixFactory.create(input, 2, 3);

    final Matrix m1 = m.add(1.0F);
    assertArrayEquals(output1, m1.toFloatArray());
    m1.addi(-1.0F);
    assertArrayEquals(input, m1.toFloatArray());
    MatrixUtils.free(m1);

    final Matrix m2 = m.sub(-1.0F);
    assertArrayEquals(output1, m2.toFloatArray());
    m2.subi(1.0F);
    assertArrayEquals(input, m2.toFloatArray());
    MatrixUtils.free(m2);

    final Matrix m3 = m.mul(2.0F);
    assertArrayEquals(output2, m3.toFloatArray());
    m3.muli(0.5F);
    assertArrayEquals(input, m3.toFloatArray());
    MatrixUtils.free(m3);

    final Matrix m4 = m.div(0.5F);
    assertArrayEquals(output2, m4.toFloatArray());
    m4.divi(2.0F);
    assertArrayEquals(input, m4.toFloatArray());
    MatrixUtils.free(m4);

    final Matrix m5 = m.rsub(1.0F);
    assertArrayEquals(output3, m5.toFloatArray());
    m5.rsubi(1.0F);
    assertArrayEquals(input, m5.toFloatArray());
    MatrixUtils.free(m5);

    final Matrix m6 = m.rdiv(0.6F);
    assertArrayEquals(output4, m6.toFloatArray());
    m6.rdivi(0.6F);
    assertArrayEquals(input, m6.toFloatArray());
    MatrixUtils.free(m6);

    MatrixUtils.free(m);
  }

  /**
   * Tests matrix-columnVector operations.
   */
  @Test
  public void testColumnVectorOps() {
    final float[] input1 = {0.1F, 0.2F, 0.3F};
    final float[] input2 = {0.6F, 0.6F, 0.6F, 0.0F, 0.0F, 0.0F};
    final float[] output1 = {0.6F, 0.6F, 0.6F, 0.1F, 0.2F, 0.3F};
    final float[] output2 = {0.7F, 0.8F, 0.9F, 0.2F, 0.4F, 0.6F};
    final float[] output3 = {0.5F, 0.4F, 0.3F, 0.0F, 0.0F, 0.0F};
    final float[] output4 = {0.06F, 0.12F, 0.18F, 0.01F, 0.04F, 0.09F};
    final float[] output5 = {6.0F, 3.0F, 2.0F, 1.0F, 1.0F, 1.0F};
    final Matrix v = matrixFactory.create(input1);
    final Matrix m = matrixFactory.create(input2, 3, 2);
    m.putColumn(1, v);

    final Matrix v1 = m.getColumn(1);
    assertArrayEquals(input1, v1.toFloatArray());
    MatrixUtils.free(v1);

    final Matrix m1 = m.addColumnVector(v);
    assertArrayEquals(output2, m1.toFloatArray());
    m1.subiColumnVector(v);
    assertArrayEquals(output1, m1.toFloatArray());
    MatrixUtils.free(m1);

    final Matrix m2 = m.subColumnVector(v);
    assertArrayEquals(output3, m2.toFloatArray());
    m2.addiColumnVector(v);
    assertArrayEquals(output1, m2.toFloatArray());
    MatrixUtils.free(m2);

    final Matrix m3 = m.mulColumnVector(v);
    assertArrayEquals(output4, m3.toFloatArray());
    m3.diviColumnVector(v);
    assertArrayEquals(output1, m3.toFloatArray());
    MatrixUtils.free(m3);

    final Matrix m4 = m.divColumnVector(v);
    assertArrayEquals(output5, m4.toFloatArray());
    m4.muliColumnVector(v);
    assertArrayEquals(output1, m4.toFloatArray());
    MatrixUtils.free(m4);

    MatrixUtils.free(m);
  }

  /**
   * Tests matrix-rowVector operations.
   */
  @Test
  public void testRowVectorOps() {
    final float[] input1 = {0.1F, 0.2F, 0.3F};
    final float[] input2 = {0.6F, 0.0F, 0.6F, 0.0F, 0.6F, 0.0F};
    final float[] output1 = {0.6F, 0.1F, 0.6F, 0.2F, 0.6F, 0.3F};
    final float[] output2 = {0.7F, 0.2F, 0.8F, 0.4F, 0.9F, 0.6F};
    final float[] output3 = {0.5F, 0.0F, 0.4F, 0.0F, 0.3F, 0.0F};
    final float[] output4 = {0.06F, 0.01F, 0.12F, 0.04F, 0.18F, 0.09F};
    final float[] output5 = {6.0F, 1.0F, 3.0F, 1.0F, 2.0F, 1.0F};
    final Matrix v = matrixFactory.create(input1, 1, 3);
    final Matrix m = matrixFactory.create(input2, 2, 3);
    m.putRow(1, v);

    final Matrix v1 = m.getRow(1);
    assertArrayEquals(input1, v1.toFloatArray());
    MatrixUtils.free(v1);

    final Matrix m1 = m.addRowVector(v);
    assertArrayEquals(output2, m1.toFloatArray());
    m1.subiRowVector(v);
    assertArrayEquals(output1, m1.toFloatArray());
    MatrixUtils.free(m1);

    final Matrix m2 = m.subRowVector(v);
    assertArrayEquals(output3, m2.toFloatArray());
    m2.addiRowVector(v);
    assertArrayEquals(output1, m2.toFloatArray());
    MatrixUtils.free(m2);

    final Matrix m3 = m.mulRowVector(v);
    assertArrayEquals(output4, m3.toFloatArray());
    m3.diviRowVector(v);
    assertArrayEquals(output1, m3.toFloatArray());
    MatrixUtils.free(m3);

    final Matrix m4 = m.divRowVector(v);
    assertArrayEquals(output5, m4.toFloatArray());
    m4.muliRowVector(v);
    assertArrayEquals(output1, m4.toFloatArray());
    MatrixUtils.free(m4);

    MatrixUtils.free(m);
  }

  /**
   * Tests matrix-matrix element-wise operations.
   */
  @Test
  public void testElementWiseOps() {
    final float[] input1 = {1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F};
    final float[] input2 = {0.1F, 0.2F, 0.3F, 0.1F, 0.2F, 0.3F};
    final float[] output1 = {1.1F, 2.2F, 3.3F, 4.1F, 5.2F, 6.3F};
    final float[] output2 = {0.9F, 1.8F, 2.7F, 3.9F, 4.8F, 5.7F};
    final float[] output3 = {0.1F, 0.4F, 0.9F, 0.4F, 1.0F, 1.8F};
    final float[] output4 = {10.0F, 10.0F, 10.0F, 40.0F, 25.0F, 20.0F};
    final Matrix m1 = matrixFactory.create(input1, 2, 3);
    final Matrix m2 = matrixFactory.create(input2, 2, 3);

    final Matrix m3 = m1.add(m2);
    assertArrayEquals(output1, m3.toFloatArray());
    m3.subi(m2);
    assertArrayEquals(input1, m3.toFloatArray());
    MatrixUtils.free(m3);

    final Matrix m4 = m1.sub(m2);
    assertArrayEquals(output2, m4.toFloatArray());
    m4.addi(m2);
    assertArrayEquals(input1, m4.toFloatArray());
    MatrixUtils.free(m4);

    final Matrix m5 = m1.mul(m2);
    assertArrayEquals(output3, m5.toFloatArray());
    m5.divi(m2);
    assertArrayEquals(input1, m5.toFloatArray());
    MatrixUtils.free(m5);

    final Matrix m6 = m1.div(m2);
    assertArrayEquals(output4, m6.toFloatArray());
    m6.muli(m2);
    assertArrayEquals(input1, m6.toFloatArray());
    MatrixUtils.free(m6);

    final Matrix m7 = m2.rsub(m1);
    assertArrayEquals(output2, m7.toFloatArray());
    m7.rsubi(m1);
    assertArrayEquals(input2, m7.toFloatArray());
    MatrixUtils.free(m7);

    final Matrix m8 = m2.rdiv(m1);
    assertArrayEquals(output4, m8.toFloatArray());
    m8.rdivi(m1);
    assertArrayEquals(input2, m8.toFloatArray());
    MatrixUtils.free(m8);

    final Matrix m9 = m1.add(m2);
    assertArrayEquals(output1, m9.toFloatArray());
    final Matrix m10 = matrixFactory.create(m9.getRows(), m9.getColumns());
    m9.sub(m2, m10);
    assertArrayEquals(input1, m10.toFloatArray());
    MatrixUtils.free(m9);
    MatrixUtils.free(m10);

    final Matrix m11 = matrixFactory.create(m1.getRows(), m2.getColumns());
    m1.mul(m2, m11);
    assertArrayEquals(output3, m11.toFloatArray());
    MatrixUtils.free(m11);

    MatrixUtils.free(m1);
    MatrixUtils.free(m2);
  }

  /**
   * Tests matrix-matrix multiplication.
   */
  @Test
  public void testMatrixMultiplication() {
    final float[] input1 = {1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F};
    final float[] input2 = {0.1F, 0.2F, 0.3F, 0.4F, 0.5F, 0.6F, 0.7F, 0.8F, 0.9F, 1.0F, 1.1F, 1.2F};
    final float[] output = {2.2F, 2.8F, 4.9F, 6.4F, 7.6F, 10.0F, 10.3F, 13.6F};

    final Matrix m1 = matrixFactory.create(input1, 2, 3);
    final Matrix m2 = matrixFactory.create(input2, 3, 4);
    final Matrix m3 = m1.mmul(m2);
    final Matrix m4 = matrixFactory.create(2, 4);
    m1.mmul(m2, m4);
    assertTrue(m4.compare(m3, 2.0E-7F));
    assertArrayEquals(output, m4.toFloatArray());
    MatrixUtils.free(m1);
    MatrixUtils.free(m2);
    MatrixUtils.free(m3);
    MatrixUtils.free(m4);
  }

  @Test
  public void testMatrixTransposeMultiplication() {
    final float[] input1 = {1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F};
    final float[] input2 = {0.1F, 0.2F, 0.3F, 0.4F, 0.5F, 0.6F, 0.7F, 0.8F, 0.9F, 1.0F, 1.1F, 1.2F};
    final float[] input3 = {7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F};
    final float[] input4 = {1.0F, 2.0F};
    //input1 * (input2 ^T)
    final float[] outputt = {6.1F, 7.6F, 7.0F, 8.8F, 7.9F, 10.0F, 8.8F, 11.2F};
    //(input1 ^T) * input3
    final float[] toutput = {23.0F, 53.0F, 83.0F, 29.0F, 67.0F, 105.0F, 35.0F, 81.0F, 127.0F};
    //(input1 ^T) * input4
    final float[] voutput = {5.0F, 11.0F, 17.0F};

    final Matrix m1 = matrixFactory.create(input1, 2, 3);
    final Matrix m2 = matrixFactory.create(input2, 4, 3);
    final Matrix m3 = matrixFactory.create(input3, 2, 3);
    final Matrix m4 = matrixFactory.create(input4, 2, 1);

    final Matrix m8 = m1.mmult(m2);
    assertArrayEquals(outputt, m8.toFloatArray());
    MatrixUtils.free(m8);

    final Matrix m9 = m1.tmmul(m3);
    assertArrayEquals(toutput, m9.toFloatArray());
    MatrixUtils.free(m9);

    final Matrix m10 = m1.tmmul(m4);
    assertArrayEquals(voutput, m10.toFloatArray());
    MatrixUtils.free(m10);

    final Matrix m11 = matrixFactory.create(m1.getRows(), m2.getRows());
    m1.mmult(m2, m11);
    assertArrayEquals(outputt, m11.toFloatArray());
    MatrixUtils.free(m11);

    final Matrix m12 = matrixFactory.create(m1.getColumns(), m3.getColumns());
    m1.tmmul(m3, m12);
    assertArrayEquals(toutput, m12.toFloatArray());
    MatrixUtils.free(m12);

    final Matrix m13 = matrixFactory.create(m1.getColumns(), m4.getColumns());
    m1.tmmul(m4, m13);
    assertArrayEquals(voutput, m13.toFloatArray());
    MatrixUtils.free(m13);

    MatrixUtils.free(m1);
    MatrixUtils.free(m2);
    MatrixUtils.free(m3);
    MatrixUtils.free(m4);
  }
}
