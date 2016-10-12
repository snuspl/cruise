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
package edu.snu.cay.dolphin.async.dnn.blas.jblas;

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.SynchronizedRandomGenerator;
import org.jblas.FloatMatrix;
import org.jblas.NativeBlas;

/**
 * Matrix implementation based on JBLAS.
 */
final class MatrixJBLASImpl implements Matrix {

  private final FloatMatrix jblasMatrix;
  private final SynchronizedRandomGenerator randomGenerator;

  MatrixJBLASImpl(final FloatMatrix jblasMatrix) {
    this.jblasMatrix = jblasMatrix;
    this.randomGenerator = new SynchronizedRandomGenerator(new MersenneTwister());
  }

  @Override
  public int getRows() {
    return jblasMatrix.getRows();
  }

  @Override
  public int getColumns() {
    return jblasMatrix.getColumns();
  }

  @Override
  public float get(final int index) {
    return jblasMatrix.get(index);
  }

  @Override
  public Matrix get(final int[] indices) {
    return new MatrixJBLASImpl(jblasMatrix.get(indices));
  }

  @Override
  public float get(final int rowIndex, final int columnIndex) {
    return jblasMatrix.get(rowIndex, columnIndex);
  }

  @Override
  public Matrix put(final int index, final float value) {
    jblasMatrix.put(index, value);
    return this;
  }

  @Override
  public Matrix put(final int rowIndex, final int columnIndex, final float value) {
    jblasMatrix.put(rowIndex, columnIndex, value);
    return this;
  }

  @Override
  public void putColumn(final int index, final Matrix vector) {
    checkImpl(vector);
    jblasMatrix.putColumn(index, ((MatrixJBLASImpl) vector).jblasMatrix);
  }

  @Override
  public void putRow(final int index, final Matrix vector) {
    checkImpl(vector);
    jblasMatrix.putRow(index, ((MatrixJBLASImpl) vector).jblasMatrix);
  }

  @Override
  public Matrix getColumn(final int index) {
    return new MatrixJBLASImpl(jblasMatrix.getColumn(index));
  }

  @Override
  public Matrix getRow(final int index) {
    return new MatrixJBLASImpl(jblasMatrix.getRow(index));
  }

  @Override
  public int getLength() {
    return jblasMatrix.getLength();
  }

  @Override
  public boolean isColumnVector() {
    return jblasMatrix.isColumnVector();
  }

  @Override
  public boolean isRowVector() {
    return jblasMatrix.isRowVector();
  }

  @Override
  public Matrix fill(final float value) {
    jblasMatrix.fill(value);
    return this;
  }

  @Override
  public Matrix reshape(final int newRows, final int newColumns) {
    jblasMatrix.reshape(newRows, newColumns);
    return this;
  }

  @Override
  public String toString() {
    return jblasMatrix.toString();
  }

  @Override
  public float[] toFloatArray() {
    return jblasMatrix.toArray();
  }

  @Override
  public Matrix copy(final Matrix matrix) {
    checkImpl(matrix);
    jblasMatrix.copy(((MatrixJBLASImpl) matrix).jblasMatrix);
    return this;
  }

  @Override
  public Matrix dup() {
    return new MatrixJBLASImpl(jblasMatrix.dup());
  }

  @Override
  public Matrix transpose() {
    return new MatrixJBLASImpl(jblasMatrix.transpose());
  }

  @Override
  public Matrix add(final float value) {
    return new MatrixJBLASImpl(jblasMatrix.add(value));
  }

  @Override
  public Matrix addi(final float value) {
    jblasMatrix.addi(value);
    return this;
  }

  @Override
  public Matrix add(final Matrix matrix) {
    checkImpl(matrix);
    return new MatrixJBLASImpl(this.jblasMatrix.add(((MatrixJBLASImpl) matrix).jblasMatrix));
  }

  @Override
  public Matrix addi(final Matrix matrix) {
    checkImpl(matrix);
    this.jblasMatrix.addi(((MatrixJBLASImpl) matrix).jblasMatrix);
    return this;
  }

  @Override
  public Matrix addColumnVector(final Matrix vector) {
    checkImpl(vector);
    return new MatrixJBLASImpl(jblasMatrix.addColumnVector(((MatrixJBLASImpl) vector).jblasMatrix));
  }

  @Override
  public Matrix addiColumnVector(final Matrix vector) {
    checkImpl(vector);
    jblasMatrix.addiColumnVector(((MatrixJBLASImpl) vector).jblasMatrix);
    return this;
  }

  @Override
  public Matrix addRowVector(final Matrix vector) {
    checkImpl(vector);
    return new MatrixJBLASImpl(jblasMatrix.addRowVector(((MatrixJBLASImpl) vector).jblasMatrix));
  }

  @Override
  public Matrix addiRowVector(final Matrix vector) {
    checkImpl(vector);
    jblasMatrix.addiRowVector(((MatrixJBLASImpl) vector).jblasMatrix);
    return this;
  }

  @Override
  public Matrix sub(final float value) {
    return new MatrixJBLASImpl(jblasMatrix.sub(value));
  }

  @Override
  public Matrix subi(final float value) {
    jblasMatrix.subi(value);
    return this;
  }

  @Override
  public Matrix sub(final Matrix matrix) {
    checkImpl(matrix);
    return new MatrixJBLASImpl(jblasMatrix.sub(((MatrixJBLASImpl) matrix).jblasMatrix));
  }

  @Override
  public Matrix subi(final Matrix matrix) {
    checkImpl(matrix);
    jblasMatrix.subi(((MatrixJBLASImpl) matrix).jblasMatrix);
    return this;
  }

  @Override
  public Matrix subColumnVector(final Matrix vector) {
    checkImpl(vector);
    return new MatrixJBLASImpl(jblasMatrix.subColumnVector(((MatrixJBLASImpl) vector).jblasMatrix));
  }

  @Override
  public Matrix subiColumnVector(final Matrix vector) {
    checkImpl(vector);
    jblasMatrix.subiColumnVector(((MatrixJBLASImpl) vector).jblasMatrix);
    return this;
  }

  @Override
  public Matrix subRowVector(final Matrix vector) {
    checkImpl(vector);
    return new MatrixJBLASImpl(jblasMatrix.subRowVector(((MatrixJBLASImpl) vector).jblasMatrix));
  }

  @Override
  public Matrix subiRowVector(final Matrix vector) {
    checkImpl(vector);
    jblasMatrix.subiRowVector(((MatrixJBLASImpl) vector).jblasMatrix);
    return this;
  }

  @Override
  public Matrix rsub(final float value) {
    return new MatrixJBLASImpl(jblasMatrix.rsub(value));
  }

  @Override
  public Matrix rsubi(final float value) {
    jblasMatrix.rsubi(value);
    return this;
  }

  @Override
  public Matrix rsub(final Matrix matrix) {
    checkImpl(matrix);
    return new MatrixJBLASImpl(jblasMatrix.rsub(((MatrixJBLASImpl) matrix).jblasMatrix));
  }

  @Override
  public Matrix rsubi(final Matrix matrix) {
    checkImpl(matrix);
    jblasMatrix.rsubi(((MatrixJBLASImpl) matrix).jblasMatrix);
    return this;
  }

  @Override
  public Matrix mul(final float value) {
    return new MatrixJBLASImpl(jblasMatrix.mul(value));
  }

  @Override
  public Matrix muli(final float value) {
    jblasMatrix.muli(value);
    return this;
  }

  @Override
  public Matrix mul(final Matrix matrix) {
    checkImpl(matrix);
    return new MatrixJBLASImpl(jblasMatrix.mul(((MatrixJBLASImpl) matrix).jblasMatrix));
  }

  @Override
  public Matrix mul(final Matrix matrix, final Matrix result) {
    checkImpl(matrix);
    checkImpl(result);
    jblasMatrix.muli(((MatrixJBLASImpl) matrix).jblasMatrix, ((MatrixJBLASImpl) result).jblasMatrix);
    return result;
  }

  @Override
  public Matrix muli(final Matrix matrix) {
    checkImpl(matrix);
    jblasMatrix.muli(((MatrixJBLASImpl) matrix).jblasMatrix);
    return this;
  }

  @Override
  public Matrix mulColumnVector(final Matrix vector) {
    checkImpl(vector);
    return new MatrixJBLASImpl(jblasMatrix.mulColumnVector(((MatrixJBLASImpl) vector).jblasMatrix));
  }

  @Override
  public Matrix muliColumnVector(final Matrix vector) {
    checkImpl(vector);
    jblasMatrix.muliColumnVector(((MatrixJBLASImpl) vector).jblasMatrix);
    return this;
  }

  @Override
  public Matrix mulRowVector(final Matrix vector) {
    checkImpl(vector);
    return new MatrixJBLASImpl(jblasMatrix.mulRowVector(((MatrixJBLASImpl) vector).jblasMatrix));
  }

  @Override
  public Matrix muliRowVector(final Matrix vector) {
    checkImpl(vector);
    jblasMatrix.muliRowVector(((MatrixJBLASImpl) vector).jblasMatrix);
    return this;
  }

  @Override
  public Matrix div(final float value) {
    return new MatrixJBLASImpl(jblasMatrix.div(value));
  }

  @Override
  public Matrix divi(final float value) {
    jblasMatrix.divi(value);
    return this;
  }

  @Override
  public Matrix div(final Matrix matrix) {
    checkImpl(matrix);
    return new MatrixJBLASImpl(jblasMatrix.div(((MatrixJBLASImpl) matrix).jblasMatrix));
  }

  @Override
  public Matrix divi(final Matrix matrix) {
    checkImpl(matrix);
    jblasMatrix.divi(((MatrixJBLASImpl) matrix).jblasMatrix);
    return this;
  }

  @Override
  public Matrix divColumnVector(final Matrix vector) {
    checkImpl(vector);
    return new MatrixJBLASImpl(jblasMatrix.divColumnVector(((MatrixJBLASImpl) vector).jblasMatrix));
  }

  @Override
  public Matrix diviColumnVector(final Matrix vector) {
    checkImpl(vector);
    jblasMatrix.diviColumnVector(((MatrixJBLASImpl) vector).jblasMatrix);
    return this;
  }

  @Override
  public Matrix divRowVector(final Matrix vector) {
    checkImpl(vector);
    return new MatrixJBLASImpl(jblasMatrix.divRowVector(((MatrixJBLASImpl) vector).jblasMatrix));
  }

  @Override
  public Matrix diviRowVector(final Matrix vector) {
    checkImpl(vector);
    jblasMatrix.diviRowVector(((MatrixJBLASImpl) vector).jblasMatrix);
    return this;
  }

  @Override
  public Matrix rdiv(final float value) {
    return new MatrixJBLASImpl(jblasMatrix.rdiv(value));
  }

  @Override
  public Matrix rdivi(final float value) {
    jblasMatrix.rdivi(value);
    return this;
  }

  @Override
  public Matrix rdiv(final Matrix matrix) {
    checkImpl(matrix);
    return new MatrixJBLASImpl(jblasMatrix.rdiv(((MatrixJBLASImpl) matrix).jblasMatrix));
  }

  @Override
  public Matrix rdivi(final Matrix matrix) {
    checkImpl(matrix);
    jblasMatrix.rdivi(((MatrixJBLASImpl) matrix).jblasMatrix);
    return this;
  }

  @Override
  public Matrix mmul(final Matrix matrix) {
    checkImpl(matrix);
    return new MatrixJBLASImpl(jblasMatrix.mmul(((MatrixJBLASImpl) matrix).jblasMatrix));
  }

  @Override
  public Matrix mmuli(final Matrix matrix) {
    checkImpl(matrix);
    jblasMatrix.mmuli(((MatrixJBLASImpl) matrix).jblasMatrix);
    return this;
  }

  @Override
  public Matrix mmul(final Matrix matrix, final Matrix result) {
    checkImpl(matrix);
    checkImpl(result);
    if (getRows() != result.getRows() || getColumns() != matrix.getColumns()) {
      throw new IllegalArgumentException("The size of result matrix is wrong");
    }
    jblasMatrix.mmuli(((MatrixJBLASImpl) matrix).jblasMatrix, ((MatrixJBLASImpl) result).jblasMatrix);
    return null;
  }

  @Override
  public Matrix tmmul(final Matrix matrix) {
    final Matrix newMatrix = new MatrixJBLASImpl(new FloatMatrix(getColumns(), matrix.getColumns()));
    return tmmul(matrix, newMatrix);
  }

  @Override
  public Matrix tmmul(final Matrix matrix, final Matrix result) {
    if (result.getRows() != getColumns() || result.getColumns() != matrix.getColumns()) {
      throw new IllegalArgumentException("The size of result matrix is wrong");
    }
    NativeBlas.sgemm('T', 'N', result.getRows(), result.getColumns(), getRows(), 1.0f, jblasMatrix.data, 0,
        getRows(), ((MatrixJBLASImpl) matrix).jblasMatrix.data, 0, matrix.getRows(), 0,
        ((MatrixJBLASImpl) result).jblasMatrix.data, 0, result.getRows());
    return result;
  }

  @Override
  public Matrix mmult(final Matrix matrix) {
    final Matrix newMatrix = new MatrixJBLASImpl(new FloatMatrix(getRows(), matrix.getRows()));
    return mmult(matrix, newMatrix);
  }

  @Override
  public Matrix mmult(final Matrix matrix, final Matrix result) {
    if (result.getRows() != getRows() || result.getColumns() != matrix.getRows()) {
      throw new IllegalArgumentException("The size of result matrix is wrong");
    }
    NativeBlas.sgemm('N', 'T', result.getRows(), result.getColumns(), getColumns(), 1.0f, jblasMatrix.data, 0,
        getRows(), ((MatrixJBLASImpl) matrix).jblasMatrix.data, 0, matrix.getRows(), 0,
        ((MatrixJBLASImpl) result).jblasMatrix.data, 0, result.getRows());
    return result;
  }

  @Override
  public float max() {
    return jblasMatrix.max();
  }

  @Override
  public Matrix columnMaxs() {
    return new MatrixJBLASImpl(jblasMatrix.columnMaxs());
  }

  @Override
  public Matrix rowMaxs() {
    return new MatrixJBLASImpl(jblasMatrix.rowMaxs());
  }

  @Override
  public float min() {
    return jblasMatrix.min();
  }

  @Override
  public Matrix columnMins() {
    return new MatrixJBLASImpl(jblasMatrix.columnMins());
  }

  @Override
  public Matrix rowMins() {
    return new MatrixJBLASImpl(jblasMatrix.rowMins());
  }

  @Override
  public Matrix columnSums() {
    return new MatrixJBLASImpl(jblasMatrix.columnSums());
  }

  @Override
  public Matrix rowSums() {
    return new MatrixJBLASImpl(jblasMatrix.rowSums());
  }

  @Override
  public Matrix rowSums(final Matrix result) {
    if (result.getRows() != getRows() || result.getColumns() != 1) {
      throw new IllegalArgumentException("The size of result matrix is wrong");
    }
    result.copy(rowSums());
    return result;
  }

  @Override
  public float sum() {
    return jblasMatrix.sum();
  }

  @Override
  public boolean hasSameSize(final Matrix matrix) {
    return getRows() == matrix.getRows() && getColumns() == matrix.getColumns();
  }

  @Override
  public boolean compare(final Matrix matrix, final float tolerance) {
    if (matrix instanceof MatrixJBLASImpl) {
      return jblasMatrix.compare(((MatrixJBLASImpl) matrix).jblasMatrix, tolerance);
    }
    return false;
  }

  @Override
  public Matrix bernoulli(final float prob, final float scale, final long seed) {
    randomGenerator.setSeed(seed);

    for (int i = 0; i < getRows(); ++i) {
      for (int j = 0; j < getColumns(); ++j) {
        if (randomGenerator.nextFloat() <= prob) {
          put(i, j, scale);
        } else {
          put(i, j, 0);
        }
      }
    }
    return this;
  }

  @Override
  public boolean equals(final Object o) {
    if (o instanceof MatrixJBLASImpl) {
      return jblasMatrix.equals(((MatrixJBLASImpl) o).jblasMatrix);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return jblasMatrix.hashCode();
  }

  public static MatrixJBLASImpl concatHorizontally(final MatrixJBLASImpl a, final MatrixJBLASImpl b) {
    return new MatrixJBLASImpl(FloatMatrix.concatHorizontally(a.jblasMatrix, b.jblasMatrix));
  }

  public static MatrixJBLASImpl concatVertically(final MatrixJBLASImpl a, final MatrixJBLASImpl b) {
    return new MatrixJBLASImpl(FloatMatrix.concatVertically(a.jblasMatrix, b.jblasMatrix));
  }

  private void checkImpl(final Matrix matrix) {
    if (!(matrix instanceof MatrixJBLASImpl)) {
      // TODO #147: different matrix implementations
      throw new IllegalArgumentException("The given matrix should be JBLAS based");
    }
  }
}
