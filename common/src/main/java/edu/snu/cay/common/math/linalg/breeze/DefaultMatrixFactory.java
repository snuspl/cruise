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

import breeze.storage.Zero;
import breeze.storage.Zero$;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import edu.snu.cay.common.math.linalg.Matrix;
import edu.snu.cay.common.math.linalg.MatrixFactory;
import edu.snu.cay.common.math.linalg.Vector;
import scala.Predef;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import javax.inject.Inject;
import java.util.List;

/**
 * Factory class for breeze based matrix.
 */
public final class DefaultMatrixFactory implements MatrixFactory {

  private static final ClassTag TAG = ClassTag$.MODULE$.Float();
  private static final Zero ZERO = Zero$.MODULE$.forClass(Float.TYPE);
  private static final Predef.$less$colon$less CONFORMS = Predef.conforms();

  @Inject
  private DefaultMatrixFactory() {
  }

  /**
   * Creates a dense matrix in which all elements are equal to {@code 0}.
   * @param rows number of rows
   * @param columns number of columns
   * @return a generated matrix
   */
  @Override
  public DenseMatrix createDenseZeros(final int rows, final int columns) {
    return new DenseMatrix(breeze.linalg.DenseMatrix.zeros(rows, columns, TAG, ZERO));
  }

  /**
   * Creates a dense matrix with the given values.
   * This method does not make a deep copy of {@code data}.
   * Thus, changes in {@code data} also change the returning matrix.
   * @param rows number of rows
   * @param columns number of columns
   * @param data elements of a matrix in column-major order
   * @return a generated matrix
   */
  @Override
  public DenseMatrix createDense(final int rows, final int columns, final Float[] data) {
    return new DenseMatrix(breeze.linalg.DenseMatrix.create(rows, columns, data, ZERO));
  }

  /**
   * Creates a CSC matrix in which all elements are equal to {@code 0}.
   * @param rows number of rows
   * @param columns number of columns
   * @param initialNonZeros number of initial nonZeros
   * @return a generated matrix
   */
  @Override
  public CSCMatrix createCSCZeros(final int rows, final int columns, final int initialNonZeros) {
    return new CSCMatrix(breeze.linalg.CSCMatrix.zeros(rows, columns, initialNonZeros, TAG, ZERO));
  }

  /**
   * Creates a dense matrix by horizontal concatenation of vectors.
   * All vectors should be instances of {@link DenseVector} and have the same length.
   * @param vectors list of concatenating vectors
   * @return a generated matrix
   */
  @Override
  public DenseMatrix horzcatVecDense(final List<Vector> vectors) {
    final List<breeze.linalg.DenseVector<Float>> breezeVecList = Lists.transform(vectors,
        new Function<Vector, breeze.linalg.DenseVector<Float>>() {
          public breeze.linalg.DenseVector<Float> apply(final Vector vector) {
            return ((DenseVector) vector).getBreezeVector();
          }
        });
    return new DenseMatrix(
        breeze.linalg.DenseVector.horzcat(JavaConversions.asScalaBuffer(breezeVecList), TAG, ZERO));
  }

  /**
   * Creates a CSC matrix by horizontal concatenation of vectors.
   * All vectors should be instances of {@link SparseVector} and have the same length.
   * @param vectors list of concatenating vectors
   * @return a generated matrix
   */
  @Override
  public CSCMatrix horzcatVecSparse(final List<Vector> vectors) {
    final List<breeze.linalg.SparseVector<Float>> breezeVecList = Lists.transform(vectors,
        new Function<Vector, breeze.linalg.SparseVector<Float>>() {
          public breeze.linalg.SparseVector<Float> apply(final Vector vector) {
            return ((SparseVector) vector).getBreezeVector();
          }
        });
    return new CSCMatrix(
        breeze.linalg.SparseVector.horzcat(JavaConversions.asScalaBuffer(breezeVecList), ZERO, TAG));
  }

  /**
   * Creates a dense matrix by horizontal concatenation of matrices.
   * All matrices should be instances of {@link DenseMatrix} and have the same number of rows.
   * @param matrices list of concatenating matrices
   * @return a generated matrix
   */
  @Override
  public Matrix horzcatMatDense(final List<Matrix> matrices) {
    final List<breeze.linalg.DenseMatrix<Float>> breezeMatList = Lists.transform(matrices,
        new Function<Matrix, breeze.linalg.DenseMatrix<Float>>() {
          public breeze.linalg.DenseMatrix<Float> apply(final Matrix matrix) {
            return ((DenseMatrix) matrix).getBreezeMatrix();
          }
        });
    return new DenseMatrix(breeze.linalg.DenseMatrix.horzcat(JavaConversions.asScalaBuffer(breezeMatList),
        CONFORMS, MatrixOps.SET_DD, TAG, ZERO));
  }

  /**
   * Creates a dense matrix by vertical concatenation of matrices.
   * All matrices should be instances of {@link DenseMatrix} and have the same number of columns.
   * @param matrices list of concatenating matrices
   * @return a generated matrix
   */
  @Override
  public Matrix vertcatMatDense(final List<Matrix> matrices) {
    final List<breeze.linalg.DenseMatrix<Float>> breezeMatList = Lists.transform(matrices,
        new Function<Matrix, breeze.linalg.DenseMatrix<Float>>() {
          public breeze.linalg.DenseMatrix<Float> apply(final Matrix matrix) {
            return ((DenseMatrix) matrix).getBreezeMatrix();
          }
        });
    return new DenseMatrix(breeze.linalg.DenseMatrix.vertcat(JavaConversions.asScalaBuffer(breezeMatList),
        MatrixOps.SET_DD, TAG, ZERO));
  }
}
