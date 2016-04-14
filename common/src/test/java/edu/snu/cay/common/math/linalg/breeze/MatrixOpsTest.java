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

import com.google.common.collect.Lists;
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
  public void testDenseOps() {
    final double[] value1 = {0.1, 0.2, 0.3};
    final double[] value2 = {0.4, 0.5, 0.6};
    final double[] value3 = {1.0, 2.0};
    final double[] value4 = {-1.0, -2.0};
    final Vector vec1 = vectorFactory.createDense(value1);
    final Vector vec2 = vectorFactory.createDense(value2);
    final Vector vec3 = vectorFactory.createDense(value3);
    final Vector vec4 = vectorFactory.createDense(value4);
    final Vector vec5 = vectorFactory.createSparse(new int[]{0, 1}, value3, 2);
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
    final Matrix mat2 = matrixFactory.horzcatVecDense(denseVectorList1);
    final Matrix mat3 = matrixFactory.horzcatVecDense(denseVectorList2);

    assertEquals(mat1.add(mat2), mat2);
    assertEquals(mat1.sub(mat2), mat2.mul(-1.0));
    mat1.addi(mat2).muli(2.0).subi(mat2).muli(mat2);
    assertEquals(mat2.mul(mat2), mat1);

    assertEquals(mat2.mmul(mat3), matrixFactory.horzcatVecDense(denseVectorList3));
    assertEquals(mat2.mmul(vec3), vec1.scale(vec3.get(0)).add(vec2.scale(vec3.get(1))));
    assertEquals(mat2.mmul(vec5), mat2.mmul(vec3));

    final Matrix mat4 = matrixFactory.createDenseZeros(3, 3);
    mat4.putColumns(0, 2, mat2);
    mat4.putColumn(2, vec1);
    assertEquals(mat2, mat4.sliceColumns(0, 2));
    assertEquals(vec1, mat4.sliceColumn(2));
    final List<Vector> denseVectorList4 = Lists.newArrayList(denseVectorList1);
    denseVectorList4.add(vec1);
    assertEquals(matrixFactory.horzcatVecDense(denseVectorList4), mat4);

    final Matrix mat5 = matrixFactory.createDenseZeros(4, 2);
    mat5.putRows(0, 3, mat2);
    mat5.putRow(3, vec3);
    assertEquals(mat2, mat5.sliceRows(0, 3));
    assertEquals(vec3, mat5.sliceRow(3));
    final List<Matrix> denseMatrixList = Lists.newArrayList(mat2);
    // convert vec3 into a matrix and transpose it to make it have only one row (similar to a row vector)
    denseMatrixList.add(matrixFactory.horzcatVecDense(Lists.<Vector>newArrayList(vec3)).transpose());
    assertEquals(matrixFactory.vertcatMatDense(denseMatrixList), mat5);
  }


  /**
   * Tests CSC matrix operations work well as intended.
   */
  @Test
  public void testCSCOps() {
    final int[][] index1 = {{0, 2, 4, 6}, {1, 2, 3, 4, 5}};
    final double[][] value1 = {{0.1, 0.2, 0.3, 0.4}, {0.5, 0.6, 0.7, 0.8, 0.9}};
    final int[][] index2 = {{0, 1}, {1}, {0, 1}};
    final double[][] value2 = {{2.0, 1.0}, {0.5}, {-1.0, 0.0}};
    final Vector vec1 = vectorFactory.createSparse(index1[0], value1[0], 7);
    final Vector vec2 = vectorFactory.createSparse(index1[1], value1[1], 7);
    final Vector vec3 = vectorFactory.createSparse(index2[0], value2[0], 2);
    final Vector vec4 = vectorFactory.createSparse(index2[1], value2[1], 2);
    final Vector vec5 = vectorFactory.createSparse(index2[2], value2[2], 2);
    final Vector vec6 = vectorFactory.createDense(value2[0]);
    final List<Vector> sparseVectorList1 = new ArrayList<>();
    sparseVectorList1.add(vec1);
    sparseVectorList1.add(vec2);
    final List<Vector> sparseVectorList2 = new ArrayList<>();
    sparseVectorList2.add(vec3);
    sparseVectorList2.add(vec4);
    sparseVectorList2.add(vec5);

    final Matrix mat1 = matrixFactory.createCSCZeros(7, 2, 0);
    final Matrix mat2 = matrixFactory.horzcatVecSparse(sparseVectorList1);
    final Matrix mat3 = matrixFactory.horzcatVecSparse(sparseVectorList2);

    assertEquals(mat1.add(mat2), mat2);
    assertEquals(mat1.sub(mat2), mat2.mul(-1.0));
    mat1.addi(mat2).muli(2.0).subi(mat2).muli(mat2);
    assertEquals(mat2.mul(mat2), mat1);

    final List<Vector> sparseVectorList3 = new ArrayList<>();
    sparseVectorList3.add(vec1.scale(vec3.get(0)).add(vec2.scale(vec3.get(1))));
    sparseVectorList3.add(vec1.scale(vec4.get(0)).add(vec2.scale(vec4.get(1))));
    sparseVectorList3.add(vec1.scale(vec5.get(0)).add(vec2.scale(vec5.get(1))));

    assertEquals(mat2.mmul(mat3), matrixFactory.horzcatVecSparse(sparseVectorList3));
    assertEquals(mat2.mmul(vec3), vec1.scale(vec3.get(0)).add(vec2.scale(vec3.get(1))));
  }

  /**
   * Tests dense-CSC matrix operations work well as intended.
   */
  @Test
  public void testDenseCSCOps() {
    final int[][] index1 = {{0, 1, 2}, {0, 1, 2}};
    final int[][] index2 = {{0, 1}, {0, 1}};
    final double[][] value1 = {{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}};
    final double[][] value2 = {{1.0, 0.0}, {0.0, 1.0}};

    final List<Vector> denseVectorList1 = new ArrayList<>();
    final List<Vector> denseVectorList2 = new ArrayList<>();
    denseVectorList1.add(vectorFactory.createDense(value1[0]));
    denseVectorList1.add(vectorFactory.createDense(value1[1]));
    denseVectorList2.add(vectorFactory.createDense(value2[0]));
    denseVectorList2.add(vectorFactory.createDense(value2[1]));

    final List<Vector> sparseVectorList1 = new ArrayList<>();
    final List<Vector> sparseVectorList2 = new ArrayList<>();
    sparseVectorList1.add(vectorFactory.createSparse(index1[0], value1[0], 3));
    sparseVectorList1.add(vectorFactory.createSparse(index1[1], value1[1], 3));
    sparseVectorList2.add(vectorFactory.createSparse(index2[0], value2[0], 2));
    sparseVectorList2.add(vectorFactory.createSparse(index2[1], value2[1], 2));

    final Matrix dMat1 = matrixFactory.horzcatVecDense(denseVectorList1);
    final Matrix dMat2 = matrixFactory.horzcatVecDense(denseVectorList2);
    final Matrix sMat1 = matrixFactory.horzcatVecSparse(sparseVectorList1);
    final Matrix sMat2 = matrixFactory.horzcatVecSparse(sparseVectorList2);

    assertEquals(dMat1.add(sMat1), sMat1.addi(dMat1));
    assertEquals(sMat1.subi(dMat1), dMat1);
    assertEquals(sMat1.add(dMat1), dMat1.addi(sMat1));
    assertEquals(dMat1.subi(sMat1), sMat1);

    assertEquals(dMat1.mul(sMat1), sMat1.muli(dMat1));
    assertEquals(sMat1.divi(dMat1), dMat1);
    assertEquals(sMat1.mul(dMat1), dMat1.muli(sMat1));
    assertEquals(dMat1.divi(sMat1), sMat1);

    assertEquals(dMat1.mmul(sMat2), dMat1);
    assertEquals(sMat1.mmul(dMat2), sMat1);
  }
}
