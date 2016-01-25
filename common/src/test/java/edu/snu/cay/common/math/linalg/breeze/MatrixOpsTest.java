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
package edu.snu.cay.common.math.linalg.breeze;

import edu.snu.cay.common.math.linalg.Matrix;
import edu.snu.cay.common.math.linalg.MatrixFactory;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * This tests matrix operations.
 */
public final class MatrixOpsTest {

  private MatrixFactory matrixFactory;
  private VectorFactory vectorFactory;

  @Before
  public void setUp() {
    try {
      matrixFactory = Tang.Factory.getTang().newInjector().getInstance(MatrixFactory.class);
      vectorFactory = Tang.Factory.getTang().newInjector().getInstance(VectorFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Tests dense matrix operations work well as intended.
   */
  @Test
  public void testDenseMatrix() {
    final double[] value1 = {0.1, 0.2, 0.3};
    final double[] value2 = {0.4, 0.5, 0.6};
    final double[] value3 = {1.0, 2.0};
    final double[] value4 = {-1.0, -2.0};
    final Vector vec1 = vectorFactory.newDenseVector(value1);
    final Vector vec2 = vectorFactory.newDenseVector(value2);
    final Vector vec3 = vectorFactory.newDenseVector(value3);
    final Vector vec4 = vectorFactory.newDenseVector(value4);
    final Vector vec5 = vectorFactory.newSparseVector(new int[]{0, 1}, value3, 2);
    final List<Vector> denseVectorList1 = new ArrayList<>();
    denseVectorList1.add(vec1);
    denseVectorList1.add(vec2);
    final List<Vector> denseVectorList2 = new ArrayList<>();
    denseVectorList2.add(vec3);
    denseVectorList2.add(vec4);
    final List<Vector> denseVectorList3 = new ArrayList<>();
    denseVectorList3.add(vec1.scale(value3[0]).add(vec2.scale(value3[1])));
    denseVectorList3.add(vec1.scale(value4[0]).add(vec2.scale(value4[1])));

    final Matrix mat1 = matrixFactory.createDenseZeros(3, 2);
    final Matrix mat2 = matrixFactory.horzcatDense(denseVectorList1);
    final Matrix mat3 = matrixFactory.horzcatDense(denseVectorList2);

    assertEquals(mat1.add(mat2), mat2);
    assertEquals(mat1.sub(mat2), mat2.mul(-1.0));
    mat1.addi(mat2).muli(2.0).subi(mat2).muli(mat2);
    assertEquals(mat2.mul(mat2), mat1);

    assertEquals(mat2.mmul(mat3), matrixFactory.horzcatDense(denseVectorList3));
    assertEquals(mat2.mmul(vec3), vec1.scale(vec3.get(0)).add(vec2.scale(vec3.get(1))));
    assertEquals(mat2.mmul(vec5), mat2.mmul(vec3));
  }


  /**
   * Tests CSC matrix operations work well as intended.
   */
  @Test
  public void testCSCMatrix() {
    final int[][] index1 = {{0, 2, 4, 6}, {1, 2, 3, 4, 5}};
    final double[][] value1 = {{0.1, 0.2, 0.3, 0.4}, {0.5, 0.6, 0.7, 0.8, 0.9}};
    final int[][] index2 = {{0, 1}, {1}, {0, 1}};
    final double[][] value2 = {{2.0, 1.0}, {0.5}, {-1.0, 0.0}};
    final Vector vec1 = vectorFactory.newSparseVector(index1[0], value1[0], 7);
    final Vector vec2 = vectorFactory.newSparseVector(index1[1], value1[1], 7);
    final Vector vec3 = vectorFactory.newSparseVector(index2[0], value2[0], 2);
    final Vector vec4 = vectorFactory.newSparseVector(index2[1], value2[1], 2);
    final Vector vec5 = vectorFactory.newSparseVector(index2[2], value2[2], 2);
    final Vector vec6 = vectorFactory.newDenseVector(value2[0]);
    final List<Vector> sparseVectorList1 = new ArrayList<>();
    sparseVectorList1.add(vec1);
    sparseVectorList1.add(vec2);
    final List<Vector> sparseVectorList2 = new ArrayList<>();
    sparseVectorList2.add(vec3);
    sparseVectorList2.add(vec4);
    sparseVectorList2.add(vec5);

    final Matrix mat1 = matrixFactory.createCSCZeros(7, 2, 0);
    final Matrix mat2 = matrixFactory.horzcatSparse(sparseVectorList1);
    final Matrix mat3 = matrixFactory.horzcatSparse(sparseVectorList2);

    assertEquals(mat1.add(mat2), mat2);
    assertEquals(mat1.sub(mat2), mat2.mul(-1.0));
    mat1.addi(mat2).muli(2.0).subi(mat2).muli(mat2);
    assertEquals(mat2.mul(mat2), mat1);

    final List<Vector> sparseVectorList3 = new ArrayList<>();
    sparseVectorList3.add(vec1.scale(vec3.get(0)).add(vec2.scale(vec3.get(1))));
    sparseVectorList3.add(vec1.scale(vec4.get(0)).add(vec2.scale(vec4.get(1))));
    sparseVectorList3.add(vec1.scale(vec5.get(0)).add(vec2.scale(vec5.get(1))));

    assertEquals(mat2.mmul(mat3), matrixFactory.horzcatSparse(sparseVectorList3));
    assertEquals(mat2.mmul(vec3), vec1.scale(vec3.get(0)).add(vec2.scale(vec3.get(1))));
    assertEquals(mat2.mmul(vec6), mat2.mmul(vec3));
  }
}
