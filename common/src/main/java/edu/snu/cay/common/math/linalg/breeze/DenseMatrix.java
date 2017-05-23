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
import scala.runtime.RichInt;

/**
 * Matrix implementation based on breeze dense matrix.
 * This class should be initialized by {@link edu.snu.cay.common.math.linalg.MatrixFactory}.
 * Linear indexing is in column-major order, unless the matrix is transposed.
 */
public class DenseMatrix implements Matrix {

  private final breeze.linalg.DenseMatrix<Float> breezeMatrix;

  DenseMatrix(final breeze.linalg.DenseMatrix<Float> breezeMatrix) {
    this.breezeMatrix = breezeMatrix;
  }

  breeze.linalg.DenseMatrix<Float> getBreezeMatrix() {
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
   * Returns the element specified by the row and column indices.
   * @param rowIndex an index in range [0, rows)
   * @param columnIndex an index in range [0, columns)
   * @return element specified by given indices
   */
  @Override
  public Float get(final int rowIndex, final int columnIndex) {
    return breezeMatrix.apply(rowIndex, columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DenseVector sliceColumn(final int index) {
    return new DenseVector((breeze.linalg.DenseVector<Float>)
        breezeMatrix.apply(MatrixOps.COLON_COLON, index, MatrixOps.SLICE_COL_D));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DenseVector sliceRow(final int index) {
    return new DenseVector((breeze.linalg.DenseVector<Float>) ((breeze.linalg.Transpose)
        breezeMatrix.apply(index, MatrixOps.COLON_COLON, MatrixOps.SLICE_ROW_D)).inner());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DenseMatrix sliceColumns(final int start, final int end) {
    return new DenseMatrix((breeze.linalg.DenseMatrix<Float>)
        breezeMatrix.apply(MatrixOps.COLON_COLON, new RichInt(start).until(end), MatrixOps.SLICE_COLS_D));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DenseMatrix sliceRows(final int start, final int end) {
    return new DenseMatrix((breeze.linalg.DenseMatrix<Float>)
        breezeMatrix.apply(new RichInt(start).until(end), MatrixOps.COLON_COLON, MatrixOps.SLICE_ROWS_D));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putColumn(final int index, final Vector vector) {
    if (vector.isDense()) {
      ((NumericOps) breezeMatrix.apply(MatrixOps.COLON_COLON, index, MatrixOps.SLICE_COL_D))
          .$colon$eq(((DenseVector) vector).getBreezeVector(), VectorOps.SET_DD);
    } else {
      ((NumericOps) breezeMatrix.apply(MatrixOps.COLON_COLON, index, MatrixOps.SLICE_COL_D))
          .$colon$eq(((SparseVector) vector).getBreezeVector(), VectorOps.SET_DS);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putRow(final int index, final Vector vector) {
    if (vector.isDense()) {
      ((NumericOps) ((breeze.linalg.Transpose)
          breezeMatrix.apply(index, MatrixOps.COLON_COLON, MatrixOps.SLICE_ROW_D)).inner())
          .$colon$eq(((DenseVector) vector).getBreezeVector(), VectorOps.SET_DD);
    } else {
      ((NumericOps) ((breeze.linalg.Transpose)
          breezeMatrix.apply(index, MatrixOps.COLON_COLON, MatrixOps.SLICE_ROW_D)).inner())
          .$colon$eq(((SparseVector) vector).getBreezeVector(), VectorOps.SET_DS);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putColumns(final int start, final int end, final Matrix matrix) {
    if (matrix.isDense()) {
      ((NumericOps) breezeMatrix.apply(MatrixOps.COLON_COLON, new RichInt(start).until(end), MatrixOps.SLICE_COLS_D))
          .$colon$eq(((DenseMatrix) matrix).breezeMatrix, MatrixOps.SET_DD);
    } else {
      ((NumericOps) breezeMatrix.apply(MatrixOps.COLON_COLON, new RichInt(start).until(end), MatrixOps.SLICE_COLS_D))
          .$colon$eq(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.SET_DS);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putRows(final int start, final int end, final Matrix matrix) {
    if (matrix.isDense()) {
      ((NumericOps) breezeMatrix.apply(new RichInt(start).until(end), MatrixOps.COLON_COLON, MatrixOps.SLICE_ROWS_D))
          .$colon$eq(((DenseMatrix) matrix).breezeMatrix, MatrixOps.SET_DD);
    } else {
      ((NumericOps) breezeMatrix.apply(new RichInt(start).until(end), MatrixOps.COLON_COLON, MatrixOps.SLICE_ROWS_D))
          .$colon$eq(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.SET_DS);
    }
  }

  /**
   * Sets a matrix element.
   * @param rowIndex an index in range [0, rows)
   * @param columnIndex an index in range [0, columns)
   * @param value given value
   */
  @Override
  public void set(final int rowIndex, final int columnIndex, final Float value) {
    breezeMatrix.update(rowIndex, columnIndex, value);
  }

  /**
   * Transpose this matrix.
   * @return transposed copy of this matrix
   */
  @Override
  public Matrix transpose() {
    return new DenseMatrix((breeze.linalg.DenseMatrix<Float>) breezeMatrix.t(MatrixOps.T_D));
  }

  /**
   * Returns true if transposed, false otherwise.
   * @return true if transposed, false otherwise
   */
  public boolean isTranspose() {
    return breezeMatrix.isTranspose();
  }

  /**
   * Converts this matrix to a flat array (column-major).
   * @return flat array with matrix elements
   */
  public Float[] toArray() {
    return (Float[]) (breezeMatrix.toArray());
  }

  /**
   * Returns a new matrix same as this one.
   * @return a new copy of this matrix
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
    if (this == o) {
      return true;
    } else if (o instanceof DenseMatrix) {
      return breezeMatrix.equals(((DenseMatrix) o).breezeMatrix);
    } else if (o instanceof CSCMatrix) {
      return breezeMatrix.equals(((CSCMatrix) o).getBreezeMatrix());
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
   * @return new {@link DenseMatrix} with operation result
   */
  @Override
  public Matrix add(final Float value) {
    return new DenseMatrix((breeze.linalg.DenseMatrix<Float>) breezeMatrix.$plus(value, MatrixOps.ADD_DT));
  }

  /**
   * Adds a scalar to all elements (in place).
   * @param value operand scalar
   * @return this matrix with operation result
   */
  @Override
  public Matrix addi(final Float value) {
    ((NumericOps) breezeMatrix).$plus$eq(value, MatrixOps.ADDI_DT);
    return this;
  }

  /**
   * Adds a matrix, element-wise.
   * @param matrix operand matrix
   * @return new {@link DenseMatrix} with operation result
   */
  @Override
  public Matrix add(final Matrix matrix) {
    if (matrix.isDense()) {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Float>)
          breezeMatrix.$plus(((DenseMatrix) matrix).breezeMatrix, MatrixOps.ADD_DD));
    } else {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Float>)
          breezeMatrix.$plus(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.ADD_DS));
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
      ((NumericOps) breezeMatrix).$plus$eq(((DenseMatrix) matrix).breezeMatrix, MatrixOps.ADDI_DD);
    } else {
      ((NumericOps) breezeMatrix).$plus$eq(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.ADDI_DS);
    }
    return this;
  }

  /**
   * Subtracts a scalar from all elements.
   * @param value operand scalar
   * @return new {@link DenseMatrix} with operation result
   */
  @Override
  public Matrix sub(final Float value) {
    return new DenseMatrix((breeze.linalg.DenseMatrix<Float>) breezeMatrix.$minus(value, MatrixOps.SUB_DT));
  }

  /**
   * Subtracts a scalar from all elements (in place).
   * @param value operand scalar
   * @return this matrix with operation result
   */
  @Override
  public Matrix subi(final Float value) {
    ((NumericOps) breezeMatrix).$minus$eq(value, MatrixOps.SUBI_DT);
    return this;
  }

  /**
   * Subtracts a matrix from this matrix, element-wise.
   * @param matrix operand matrix
   * @return new {@link DenseMatrix} with operation result
   */
  @Override
  public Matrix sub(final Matrix matrix) {
    if (matrix.isDense()) {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Float>)
          breezeMatrix.$minus(((DenseMatrix) matrix).breezeMatrix, MatrixOps.SUB_DD));
    } else {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Float>)
          breezeMatrix.$minus(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.SUB_DS));
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
      ((NumericOps) breezeMatrix).$minus$eq(((DenseMatrix) matrix).breezeMatrix, MatrixOps.SUBI_DD);
    } else {
      ((NumericOps) breezeMatrix).$minus$eq(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.SUBI_DS);
    }
    return this;
  }

  /**
   * Multiplies all elements by a scalar.
   * @param value operand scalar
   * @return new {@link DenseMatrix} with operation result
   */
  @Override
  public Matrix mul(final Float value) {
    return new DenseMatrix((breeze.linalg.DenseMatrix<Float>) breezeMatrix.$colon$times(value, MatrixOps.MUL_DT));
  }

  /**
   * Multiplies all elements by a scalar (in place).
   * @param value operand scalar
   * @return this matrix with operation result
   */
  @Override
  public Matrix muli(final Float value) {
    ((NumericOps) breezeMatrix).$colon$times$eq(value, MatrixOps.MULI_DT);
    return this;
  }

  /**
   * Multiplies this matrix by another matrix, element-wise.
   * @param matrix operand matrix
   * @return new {@link DenseMatrix} operation result
   */
  @Override
  public Matrix mul(final Matrix matrix) {
    if (matrix.isDense()) {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Float>)
          breezeMatrix.$colon$times(((DenseMatrix) matrix).breezeMatrix, MatrixOps.EMUL_DD));
    } else {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Float>)
          breezeMatrix.$colon$times(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.EMUL_MM));
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
      ((NumericOps) breezeMatrix).$colon$times$eq(((DenseMatrix) matrix).breezeMatrix, MatrixOps.EMULI_DD);
    } else {
      ((NumericOps) breezeMatrix).$colon$times$eq(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.EMULI_MM);
    }
    return this;
  }

  /**
   * Divides all elements by a scalar.
   * @param value operand scalar
   * @return new {@link DenseMatrix} with operation result
   */
  @Override
  public Matrix div(final Float value) {
    return new DenseMatrix((breeze.linalg.DenseMatrix<Float>) breezeMatrix.$div(value, MatrixOps.DIV_DT));
  }

  /**
   * Divides all elements by a scalar (in place).
   * @param value operand scalar
   * @return this matrix with operation result
   */
  @Override
  public Matrix divi(final Float value) {
    ((NumericOps) breezeMatrix).$div$eq(value, MatrixOps.DIVI_DT);
    return this;
  }

  /**
   * Divides this matrix by another matrix, element-wise.
   * @param matrix operand matrix
   * @return new {@link DenseMatrix} operation result
   */
  @Override
  public Matrix div(final Matrix matrix) {
    if (matrix.isDense()) {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Float>)
          breezeMatrix.$div(((DenseMatrix) matrix).breezeMatrix, MatrixOps.EDIV_DD));
    } else {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Float>)
          breezeMatrix.$div(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.EDIV_MM));
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
      ((NumericOps) breezeMatrix).$div$eq(((DenseMatrix) matrix).breezeMatrix, MatrixOps.EDIVI_DD);
    } else {
      ((NumericOps) breezeMatrix).$div$eq(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.EDIVI_MM);
    }
    return this;
  }

  /**
   * Matrix-Vector multiplication.
   * @param vector operand vector
   * @return new {@link DenseVector} with operation result
   */
  @Override
  public Vector mmul(final Vector vector) {
    if (vector.isDense()) {
      return new DenseVector((breeze.linalg.DenseVector<Float>)
          breezeMatrix.$times(((DenseVector) vector).getBreezeVector(), MatrixOps.MUL_DMDV));
    } else {
      return new DenseVector((breeze.linalg.DenseVector<Float>)
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
      return new DenseMatrix((breeze.linalg.DenseMatrix<Float>)
          breezeMatrix.$times(((DenseMatrix) matrix).breezeMatrix, MatrixOps.MUL_DMDM));
    } else {
      return new DenseMatrix((breeze.linalg.DenseMatrix<Float>)
          breezeMatrix.$times(((CSCMatrix) matrix).getBreezeMatrix(), MatrixOps.MUL_DMSM));
    }
  }
}
