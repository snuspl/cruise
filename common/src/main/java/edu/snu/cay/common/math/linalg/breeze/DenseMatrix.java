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
 * Matrix implementation based on breeze dense matrix.
 * This class should be initialized by {@link edu.snu.cay.common.math.linalg.MatrixFactory}.
 */
public class DenseMatrix implements Matrix {

  private final breeze.linalg.DenseMatrix<Double> breezeMatrix;

  DenseMatrix(final breeze.linalg.DenseMatrix<Double> breezeMatrix) {
    this.breezeMatrix = breezeMatrix;
  }

  breeze.linalg.DenseMatrix<Double> getBreezeMatrix() {
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
   * @return true
   */
  @Override
  public boolean isDense() {
    return true;
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
   * Returns a element specified by the row and column indices.
   * @param rowIndex an index in range [0, rows)
   * @param columnIndex an index in range [0, columns)
   * @return element specified by given indices
   */
  @Override
  public double get(final int rowIndex, final int columnIndex) {
    return breezeMatrix.apply(rowIndex, columnIndex);
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
    return new DenseMatrix((breeze.linalg.DenseMatrix<Double>) breezeMatrix.t(MatrixOps.T_D));
  }

  /**
   * Returns true if transposed, false otherwise.
   * @return true if transposed, false otherwise
   */
  @Override
  public boolean isTranspose() {
    return breezeMatrix.isTranspose();
  }

  /**
   * Returns a new matrix same as this one.
   * @return copied new matrix
   */
  @Override
  public Matrix copy() {
    return new DenseMatrix(breezeMatrix.copy());
  }

  @Override
  public String toString() {
    return breezeMatrix.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (o instanceof DenseMatrix) {
      return breezeMatrix.equals(((DenseMatrix) o).breezeMatrix);
    }
    if (o instanceof CSCMatrix) {
      return breezeMatrix.equals(((CSCMatrix) o).getBreezeMatrix());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return breezeMatrix.hashCode();
  }

  /**
   * Adds a scalar to all elements.
   * @param value operand scalar
   * @return new {@link DenseMatrix} with operation result
   */
  @Override
  public Matrix add(final double value) {
    return new DenseMatrix((breeze.linalg.DenseMatrix<Double>) breezeMatrix.$plus(value, MatrixOps.ADD_DT));
  }

  /**
   * Adds a scalar to all elements (in place).
   * @param value operand scalar
   * @return this matrix with operation result
   */
  @Override
  public Matrix addi(final double value) {
    breezeMatrix.$plus$eq(value, MatrixOps.ADDI_DT);
    return this;
  }

  /**
   * Element-wise adds a matrix.
   * @param matrix operand matrix
   * @return new {@link DenseMatrix} with operation result
   */
  @Override
  public Matrix add(final Matrix matrix) {
    if (matrix.isDense()) {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Double>)
          breezeMatrix.$plus(((DenseMatrix) matrix).breezeMatrix, MatrixOps.ADD_DD));
    } else {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Double>)
          breezeMatrix.$plus(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.ADD_DS));
    }
  }

  /**
   * Element-wise adds a matrix (in place).
   * @param matrix operand matrix
   * @return this matrix with operation result
   */
  @Override
  public Matrix addi(final Matrix matrix) {
    if (matrix.isDense()) {
      ((NumericOps) breezeMatrix).$plus$eq(((DenseMatrix) matrix).breezeMatrix, MatrixOps.ADDI_DD);
    } else {
      ((NumericOps) breezeMatrix).$plus$eq(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.ADDI_DS);
    }
    return this;
  }

  /**
   * Subtracts a scalar to all elements.
   * @param value operand scalar
   * @return new {@link DenseMatrix} with operation result
   */
  @Override
  public Matrix sub(final double value) {
    return new DenseMatrix((breeze.linalg.DenseMatrix<Double>) breezeMatrix.$minus(value, MatrixOps.SUB_DT));
  }

  /**
   * Subtracts a scalar to all elements (in place).
   * @param value operand scalar
   * @return this matrix with operation result
   */
  @Override
  public Matrix subi(final double value) {
    breezeMatrix.$minus$eq(value, MatrixOps.SUBI_DT);
    return this;
  }

  /**
   * Element-wise subtracts a matrix.
   * @param matrix operand matrix
   * @return new {@link DenseMatrix} with operation result
   */
  @Override
  public Matrix sub(final Matrix matrix) {
    if (matrix.isDense()) {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Double>)
          breezeMatrix.$minus(((DenseMatrix) matrix).breezeMatrix, MatrixOps.SUB_DD));
    } else {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Double>)
          breezeMatrix.$minus(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.SUB_DS));
    }
  }

  /**
   * Element-wise subtracts a matrix (in place).
   * @param matrix operand matrix
   * @return this matrix with operation result
   */
  @Override
  public Matrix subi(final Matrix matrix) {
    if (matrix.isDense()) {
      ((NumericOps) breezeMatrix).$minus$eq(((DenseMatrix) matrix).breezeMatrix, MatrixOps.SUBI_DD);
    } else {
      ((NumericOps) breezeMatrix).$minus$eq(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.SUBI_DS);
    }
    return this;
  }

  /**
   * Multiplies a scalar to all elements.
   * @param value operand scalar
   * @return new {@link DenseMatrix} with operation result
   */
  @Override
  public Matrix mul(final double value) {
    return new DenseMatrix((breeze.linalg.DenseMatrix<Double>) breezeMatrix.$colon$times(value, MatrixOps.MUL_DT));
  }

  /**
   * Multiplies a scalar to all elements (in place).
   * @param value operand scalar
   * @return this matrix with operation result
   */
  @Override
  public Matrix muli(final double value) {
    ((NumericOps) breezeMatrix).$colon$times$eq(value, MatrixOps.MULI_DT);
    return this;
  }

  /**
   * Matrix-Matrix element-wise multiplication.
   * Throws {@code UnsupportedOperationException} if the operand is {@link CSCMatrix}.
   * @param matrix operand matrix
   * @return new {@link DenseMatrix} operation result
   */
  @Override
  public Matrix mul(final Matrix matrix) {
    if (matrix.isDense()) {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Double>)
          breezeMatrix.$colon$times(((DenseMatrix) matrix).breezeMatrix, MatrixOps.EMUL_DD));
    } else {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Matrix-Matrix element-wise multiplication (in place).
   * Throws {@code UnsupportedOperationException} if the operand is {@link CSCMatrix}.
   * @param matrix operand matrix
   * @return this matrix with operation result
   */
  @Override
  public Matrix muli(final Matrix matrix) {
    if (matrix.isDense()) {
      ((NumericOps) breezeMatrix).$colon$times$eq(((DenseMatrix) matrix).breezeMatrix, MatrixOps.EMULI_DD);
      return this;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Divides by a scalar to all elements.
   * @param value operand scalar
   * @return new {@link DenseMatrix} with operation result
   */
  @Override
  public Matrix div(final double value) {
    return new DenseMatrix((breeze.linalg.DenseMatrix<Double>) breezeMatrix.$div(value, MatrixOps.DIV_DT));
  }

  /**
   * Divides by a scalar to all elements (in place).
   * @param value operand scalar
   * @return this matrix with operation result
   */
  @Override
  public Matrix divi(final double value) {
    breezeMatrix.$div$eq(value, MatrixOps.DIVI_DT);
    return this;
  }

  /**
   * Element-wise divides by a matrix.
   * Throws {@code UnsupportedOperationException} if the operand is {@link CSCMatrix}.
   * @param matrix operand matrix
   * @return new {@link DenseMatrix} operation result
   */
  @Override
  public Matrix div(final Matrix matrix) {
    if (matrix.isDense()) {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Double>)
          breezeMatrix.$div(((DenseMatrix) matrix).breezeMatrix, MatrixOps.EDIV_DD));
    } else {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Element-wise divides by a matrix (in place).
   * Throws {@code UnsupportedOperationException} if the operand is {@link CSCMatrix}.
   * @param matrix operand matrix
   * @return this matrix with operation result
   */
  @Override
  public Matrix divi(final Matrix matrix) {
    if (matrix.isDense()) {
      ((NumericOps) breezeMatrix).$div$eq(((DenseMatrix) matrix).breezeMatrix, MatrixOps.EDIVI_DD);
      return this;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Matrix-Vector multiplication.
   * @param vector operand vector
   * @return new {@link DenseVector} with operation result
   */
  @Override
  public Vector mmul(final Vector vector) {
    if (vector.isDense()) {
      return new DenseVector((breeze.linalg.DenseVector<Double>)
          breezeMatrix.$times(((DenseVector) vector).getBreezeVector(), MatrixOps.MUL_DMDV));
    } else {
      return new DenseVector((breeze.linalg.DenseVector<Double>)
          breezeMatrix.$times(((SparseVector) vector).getBreezeVector(), MatrixOps.MUL_DMSV));
    }
  }

  /**
   * Matrix-Matrix multiplication.
   * @param matrix operand matrix
   * @return new {@link DenseMatrix} with operation result
   */
  @Override
  public Matrix mmul(final Matrix matrix) {
    if (matrix.isDense()) {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Double>)
          breezeMatrix.$times(((DenseMatrix) matrix).breezeMatrix, MatrixOps.MUL_DMDM));
    } else {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Double>)
          breezeMatrix.$times(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.MUL_DMSM));
    }
  }
}
