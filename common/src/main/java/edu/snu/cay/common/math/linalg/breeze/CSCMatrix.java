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

import breeze.linalg.NumericOps;
import edu.snu.cay.common.math.linalg.Matrix;
import edu.snu.cay.common.math.linalg.Vector;

/**
 * Matrix implementation based on breeze CSC(Compressed Sparse Column) matrix.
 * This class should be initialized by {@link edu.snu.cay.common.math.linalg.MatrixFactory}.
 */
public final class CSCMatrix implements Matrix {

  private final breeze.linalg.CSCMatrix<Double> breezeMatrix;

  CSCMatrix(final breeze.linalg.CSCMatrix<Double> breezeMatrix) {
    this.breezeMatrix = breezeMatrix;
  }

  breeze.linalg.CSCMatrix<Double> getBreezeMatrix() {
    return breezeMatrix;
  }

  /**
   * Returns the number of elements.
   * @return number of elements
   */
  @Override
  public int size() {
    return breezeMatrix.size();
  }

  /**
   * Returns the number of active elements.
   * @return number of active elements
   */
  @Override
  public int activeSize() {
    return breezeMatrix.activeSize();
  }

  /**
   * Returns true if the matrix is dense, false if sparse.
   * @return false
   */
  @Override
  public boolean isDense() {
    return false;
  }

  /**
   * Returns the number of rows.
   * @return number of rows
   */
  @Override
  public int getRows() {
    return breezeMatrix.rows();
  }

  /**
   * Returns the number of columns.
   * @return number of columns
   */
  @Override
  public int getColumns() {
    return breezeMatrix.cols();
  }

  /**
   * Returns the element specified by the row and column indices.
   * @param rowIndex an index in range [0, rows)
   * @param columnIndex an index in range [0, columns)
   * @return element specified by given indices
   */
  @Override
  public double get(final int rowIndex, final int columnIndex) {
    return breezeMatrix.apply(rowIndex, columnIndex);
  }

  /**
   * Returns the column specified by the index.
   * Modifying the return value also changes the original matrix.
   * Not supported for CSC matrix.
   * @param index an index in range [0, columns)
   * @return a column vector specified by given index
   */
  @Override
  public Vector sliceColumn(final int index) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the row specified by the index.
   * Modifying the return value also changes the original matrix.
   * Not supported for CSC matrix.
   * @param index an index in range [0, rows)
   * @return a row vector specified by given index
   */
  @Override
  public Vector sliceRow(final int index) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the columns specified by the index range.
   * Does not include {@code end}th column.
   * Modifying the return value also changes the original matrix.
   * Not supported for CSC matrix.
   * @param start an index in range [0, columns), less than {@code end}
   * @param end an index in range (0, columns], greater than {@code start}
   * @return a partial matrix which contains columns specified by given index range
   */
  @Override
  public Matrix sliceColumns(final int start, final int end) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the rows specified by the index range.
   * Does not include {@code end}th row.
   * Modifying the return value also changes the original matrix.
   * Not supported for CSC matrix.
   * @param start an index in range [0, rows), less than {@code end}
   * @param end an index in range (0, rows], greater than {@code start}
   * @return a partial matrix which contains rows specified by given index range
   */
  @Override
  public Matrix sliceRows(final int start, final int end) {
    throw new UnsupportedOperationException();
  }

  /**
   * Sets a matrix element.
   * @param rowIndex an index in range [0, rows)
   * @param columnIndex an index in range [0, columns)
   * @param value given value
   */
  @Override
  public void set(final int rowIndex, final int columnIndex, final double value) {
    breezeMatrix.update(rowIndex, columnIndex, value);
  }

  /**
   * Transpose this matrix.
   * @return transposed copy of this matrix
   */
  @Override
  public Matrix transpose() {
    return new CSCMatrix((breeze.linalg.CSCMatrix<Double>) breezeMatrix.t(MatrixOps.T_S));
  }

  /**
   * Returns a new matrix same as this one.
   * @return a new copy of this matrix
   */
  @Override
  public Matrix copy() {
    return new CSCMatrix(breezeMatrix.copy());
  }

  @Override
  public String toString() {
    return breezeMatrix.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    } else if (o instanceof CSCMatrix) {
      return breezeMatrix.equals(((CSCMatrix) o).breezeMatrix);
    } else if (o instanceof DenseMatrix) {
      return breezeMatrix.equals(((DenseMatrix) o).getBreezeMatrix());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return breezeMatrix.hashCode();
  }

  /**
   * Adds a scalar to all elements.
   * @param value operand scalar
   * @return new {@link CSCMatrix} with operation result
   */
  @Override
  public Matrix add(final double value) {
    return new CSCMatrix((breeze.linalg.CSCMatrix<Double>) breezeMatrix.$plus(value, MatrixOps.ADD_ST));
  }

  /**
   * Adds a scalar to all elements (in place).
   * @param value operand scalar
   * @return this matrix with operation result
   */
  @Override
  public Matrix addi(final double value) {
    ((NumericOps) breezeMatrix).$plus$eq(value, MatrixOps.ADDI_ST);
    return this;
  }

  /**
   * Adds a matrix, element-wise.
   * The result is {@link DenseMatrix} if the operand is {@link DenseMatrix},
   * {@link CSCMatrix} otherwise.
   * @param matrix operand matrix
   * @return new matrix with operation result
   */
  @Override
  public Matrix add(final Matrix matrix) {
    if (matrix.isDense()) {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Double>)
          breezeMatrix.$plus(((DenseMatrix) matrix).getBreezeMatrix(), MatrixOps.ADD_SD));
    } else {
      return new CSCMatrix((breeze.linalg.CSCMatrix<Double>)
          breezeMatrix.$plus(((CSCMatrix) matrix).breezeMatrix, MatrixOps.ADD_SS));
    }
  }

  /**
   * Adds a matrix, element-wise (in place).
   * @param matrix operand matrix
   * @return this matrix with operation result
   */
  @Override
  public Matrix addi(final Matrix matrix) {
    if (matrix.isDense()) {
      ((NumericOps) breezeMatrix).$plus$eq(((DenseMatrix) matrix).getBreezeMatrix(), MatrixOps.ADDI_MM);
    } else {
      ((NumericOps) breezeMatrix).$plus$eq(((CSCMatrix) matrix).breezeMatrix, MatrixOps.ADDI_SS);
    }
    return this;
  }

  /**
   * Subtracts a scalar from all elements.
   * @param value operand scalar
   * @return new {@link CSCMatrix} with operation result
   */
  @Override
  public Matrix sub(final double value) {
    return new CSCMatrix((breeze.linalg.CSCMatrix<Double>) breezeMatrix.$minus(value, MatrixOps.SUB_ST));
  }

  /**
   * Subtracts a scalar from all elements (in place).
   * @param value operand scalar
   * @return this matrix with operation result
   */
  @Override
  public Matrix subi(final double value) {
    ((NumericOps) breezeMatrix).$minus$eq(value, MatrixOps.SUBI_ST);
    return this;
  }

  /**
   * Subtracts a matrix from this matrix, element-wise.
   * The result is {@link DenseMatrix} if the operand is {@link DenseMatrix},
   * {@link CSCMatrix} otherwise.
   * @param matrix operand matrix
   * @return new matrix with operation result
   */
  @Override
  public Matrix sub(final Matrix matrix) {
    if (matrix.isDense()) {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Double>)
          breezeMatrix.$minus(((DenseMatrix) matrix).getBreezeMatrix(), MatrixOps.SUB_SD));
    } else {
      return new CSCMatrix((breeze.linalg.CSCMatrix<Double>)
          breezeMatrix.$minus(((CSCMatrix) matrix).breezeMatrix, MatrixOps.SUB_SS));
    }
  }

  /**
   * Subtracts a matrix from this matrix, element-wise (in place).
   * @param matrix operand matrix
   * @return this matrix with operation result
   */
  @Override
  public Matrix subi(final Matrix matrix) {
    if (matrix.isDense()) {
      ((NumericOps) breezeMatrix).$minus$eq(((DenseMatrix) matrix).getBreezeMatrix(), MatrixOps.SUBI_MM);
    } else {
      ((NumericOps) breezeMatrix).$minus$eq(((CSCMatrix) matrix).breezeMatrix, MatrixOps.SUBI_SS);
    }
    return this;
  }

  /**
   * Multiplies all elements by a scalar.
   * @param value operand scalar
   * @return new {@link CSCMatrix} with operation result
   */
  @Override
  public Matrix mul(final double value) {
    return new CSCMatrix((breeze.linalg.CSCMatrix<Double>) breezeMatrix.$colon$times(value, MatrixOps.MUL_ST));
  }

  /**
   * Multiplies all elements by a scalar (in place).
   * @param value operand scalar
   * @return this matrix with operation result
   */
  @Override
  public Matrix muli(final double value) {
    ((NumericOps) breezeMatrix).$colon$times$eq(value, MatrixOps.MULI_ST);
    return this;
  }

  /**
   * Multiplies this matrix by another matrix, element-wise.
   * @param matrix operand matrix
   * @return new {@link CSCMatrix} operation result
   */
  @Override
  public Matrix mul(final Matrix matrix) {
    if (matrix.isDense()) {
      return new CSCMatrix((breeze.linalg.CSCMatrix<Double>)
          breezeMatrix.$colon$times(((DenseMatrix) matrix).getBreezeMatrix(), MatrixOps.EMUL_MM));
    } else {
      return new CSCMatrix((breeze.linalg.CSCMatrix<Double>)
          breezeMatrix.$colon$times(((CSCMatrix) matrix).breezeMatrix, MatrixOps.EMUL_SS));
    }
  }

  /**
   * Multiplies this matrix by another matrix, element-wise (in place).
   * @param matrix operand matrix
   * @return this matrix with operation result
   */
  @Override
  public Matrix muli(final Matrix matrix) {
    if (matrix.isDense()) {
      ((NumericOps) breezeMatrix).$colon$times$eq(((DenseMatrix) matrix).getBreezeMatrix(), MatrixOps.EMULI_MM);
    } else {
      ((NumericOps) breezeMatrix).$colon$times$eq(((CSCMatrix) matrix).breezeMatrix, MatrixOps.EMULI_SS);
    }
    return this;
  }

  /**
   * Divides all elements by a scalar.
   * @param value operand scalar
   * @return new {@link CSCMatrix} with operation result
   */
  @Override
  public Matrix div(final double value) {
    return new CSCMatrix((breeze.linalg.CSCMatrix<Double>) breezeMatrix.$div(value, MatrixOps.DIV_ST));
  }

  /**
   * Divides all elements by a scalar (in place).
   * @param value operand scalar
   * @return this matrix with operation result
   */
  @Override
  public Matrix divi(final double value) {
    ((NumericOps) breezeMatrix).$div$eq(value, MatrixOps.DIVI_ST);
    return this;
  }

  /**
   * Divides this matrix by another matrix, element-wise.
   * @param matrix operand matrix
   * @return new {@link CSCMatrix} operation result
   */
  @Override
  public Matrix div(final Matrix matrix) {
    if (matrix.isDense()) {
      return new CSCMatrix((breeze.linalg.CSCMatrix<Double>)
          breezeMatrix.$div(((DenseMatrix) matrix).getBreezeMatrix(), MatrixOps.EDIV_MM));
    } else {
      return new CSCMatrix((breeze.linalg.CSCMatrix<Double>)
          breezeMatrix.$div(((CSCMatrix) matrix).breezeMatrix, MatrixOps.EDIV_SS));
    }
  }

  /**
   * Divides this matrix by another matrix, element-wise (in place).
   * @param matrix operand matrix
   * @return this matrix with operation result
   */
  @Override
  public Matrix divi(final Matrix matrix) {
    if (matrix.isDense()) {
      ((NumericOps) breezeMatrix).$div$eq(((DenseMatrix) matrix).getBreezeMatrix(), MatrixOps.EDIVI_MM);
    } else {
      ((NumericOps) breezeMatrix).$div$eq(((CSCMatrix) matrix).breezeMatrix, MatrixOps.EDIVI_SS);
    }
    return this;
  }

  /**
   * Matrix-Vector multiplication.
   * The result is {@link DenseVector} if the operand is {@link DenseVector},
   * {@link SparseVector} otherwise.
   * @param vector operand vector
   * @return new vector with operation result
   */
  @Override
  public Vector mmul(final Vector vector) {
    if (vector.isDense()) {
      return new DenseVector((breeze.linalg.DenseVector<Double>)
          breezeMatrix.$times(((DenseVector) vector).getBreezeVector(), MatrixOps.MUL_SMDV));
    } else {
      return new SparseVector((breeze.linalg.SparseVector<Double>)
          breezeMatrix.$times(((SparseVector) vector).getBreezeVector(), MatrixOps.MUL_SMSV));
    }
  }

  /**
   * Matrix-Matrix multiplication.
   * The result is {@link DenseMatrix} if the operand is {@link DenseMatrix},
   * {@link CSCMatrix} otherwise.
   * @param matrix operand matrix
   * @return new matrix with operation result
   */
  @Override
  public Matrix mmul(final Matrix matrix) {
    if (matrix.isDense()) {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Double>)
          breezeMatrix.$times(((DenseMatrix) matrix).getBreezeMatrix(), MatrixOps.MUL_SMDM));
    } else {
      return new CSCMatrix((breeze.linalg.CSCMatrix<Double>)
          breezeMatrix.$times(((CSCMatrix) matrix).breezeMatrix, MatrixOps.MUL_SMSM));
    }
  }
}
