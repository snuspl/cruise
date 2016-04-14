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

import breeze.generic.UFunc;
import breeze.linalg.Matrix$;
import breeze.linalg.support.CanSlice2;
import breeze.linalg.support.CanTranspose;
import breeze.math.*;
import breeze.storage.Zero;
import breeze.storage.Zero$;
import scala.package$;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 * Class for breeze matrix operators.
 */
public final class MatrixOps {

  private static final ClassTag TAG = ClassTag$.MODULE$.Double();
  private static final Zero ZERO = Zero$.MODULE$.forClass(Double.TYPE);
  private static final Semiring SEMI_RING = Semiring$.MODULE$.semiringD();
  private static final Ring RING = Ring$.MODULE$.ringD();
  private static final Field FIELD = Field.fieldDouble$.MODULE$;

  private MatrixOps() {
  }

  // colon operator
  static final scala.collection.immutable.$colon$colon$ COLON_COLON = package$.MODULE$.$colon$colon();

  // set operators
  static final UFunc.InPlaceImpl2 SET_DD = breeze.linalg.DenseMatrix.dm_dm_UpdateOp_Double_OpSet();
  static final UFunc.InPlaceImpl2 SET_DS = breeze.linalg.CSCMatrix.dm_csc_InPlace_OpSet_Double();

  // slice operators
  static final CanSlice2 SLICE_COL_D = breeze.linalg.DenseMatrix.canSliceCol();
  static final CanSlice2 SLICE_ROW_D = breeze.linalg.DenseMatrix.canSliceRow();
  static final CanSlice2 SLICE_COLS_D = breeze.linalg.DenseMatrix.canSliceCols();
  static final CanSlice2 SLICE_ROWS_D = breeze.linalg.DenseMatrix.canSliceRows();

  // transpose operators
  static final CanTranspose T_D = breeze.linalg.DenseMatrix.canTranspose();
  static final CanTranspose T_S = breeze.linalg.CSCMatrix.canTranspose(TAG, ZERO, SEMI_RING);

  // scalar addition operators
  static final UFunc.UImpl2 ADD_DT = breeze.linalg.DenseMatrix.op_DM_S_Double_OpAdd();
  static final UFunc.InPlaceImpl2 ADDI_DT = breeze.linalg.DenseMatrix.dm_s_UpdateOp_Double_OpAdd();
  static final UFunc.UImpl2 ADD_ST = breeze.linalg.CSCMatrix.canAddM_S_Semiring(SEMI_RING, TAG);
  static final UFunc.InPlaceImpl2 ADDI_ST = breeze.linalg.CSCMatrix.csc_T_InPlace_Double_OpAdd();

  // matrix addition operators
  static final UFunc.UImpl2 ADD_DD = breeze.linalg.DenseMatrix.op_DM_DM_Double_OpAdd();
  static final UFunc.InPlaceImpl2 ADDI_DD = breeze.linalg.DenseMatrix.dm_dm_UpdateOp_Double_OpAdd();
  static final UFunc.UImpl2 ADD_DS = breeze.linalg.CSCMatrix.dm_csc_OpAdd_Double();
  static final UFunc.InPlaceImpl2 ADDI_DS = breeze.linalg.CSCMatrix.dm_csc_InPlace_OpAdd_Double();
  static final UFunc.UImpl2 ADD_SD = breeze.linalg.CSCMatrix.csc_dm_OpAdd_Double();
  static final UFunc.UImpl2 ADD_SS = breeze.linalg.CSCMatrix.csc_csc_OpAdd_Double();
  static final UFunc.InPlaceImpl2 ADDI_SS = breeze.linalg.CSCMatrix.csc_csc_InPlace_Double_OpAdd();
  static final UFunc.InPlaceImpl2 ADDI_MM = Matrix$.MODULE$.m_m_UpdateOp_Double_OpAdd();

  // scalar subtraction operators
  static final UFunc.UImpl2 SUB_DT = breeze.linalg.DenseMatrix.op_DM_S_Double_OpSub();
  static final UFunc.InPlaceImpl2 SUBI_DT = breeze.linalg.DenseMatrix.dm_s_UpdateOp_Double_OpSub();
  static final UFunc.UImpl2 SUB_ST = breeze.linalg.CSCMatrix.canSubM_S_Ring(RING, TAG);
  static final UFunc.InPlaceImpl2 SUBI_ST = breeze.linalg.CSCMatrix.csc_T_InPlace_Double_OpSub();

  // matrix subtraction operators
  static final UFunc.UImpl2 SUB_DD = breeze.linalg.DenseMatrix.op_DM_DM_Double_OpSub();
  static final UFunc.InPlaceImpl2 SUBI_DD = breeze.linalg.DenseMatrix.dm_dm_UpdateOp_Double_OpSub();
  static final UFunc.UImpl2 SUB_DS = breeze.linalg.CSCMatrix.dm_csc_OpSub_Double();
  static final UFunc.InPlaceImpl2 SUBI_DS = breeze.linalg.CSCMatrix.dm_csc_InPlace_OpSub_Double();
  static final UFunc.UImpl2 SUB_SD = breeze.linalg.CSCMatrix.csc_dm_OpSub_Double();
  static final UFunc.UImpl2 SUB_SS = breeze.linalg.CSCMatrix.csc_csc_OpSub_Double();
  static final UFunc.InPlaceImpl2 SUBI_SS = breeze.linalg.CSCMatrix.csc_csc_InPlace_Double_OpSub();
  static final UFunc.InPlaceImpl2 SUBI_MM = Matrix$.MODULE$.m_m_UpdateOp_Double_OpSub();

  // scalar multiplication operators
  static final UFunc.UImpl2 MUL_DT = breeze.linalg.DenseMatrix.op_DM_S_Double_OpMulScalar();
  static final UFunc.InPlaceImpl2 MULI_DT = breeze.linalg.DenseMatrix.dm_s_UpdateOp_Double_OpMulScalar();
  static final UFunc.UImpl2 MUL_ST = breeze.linalg.CSCMatrix.implOps_CSCT_T_eq_CSCT_Double_OpMulScalar();
  static final UFunc.InPlaceImpl2 MULI_ST = breeze.linalg.CSCMatrix.csc_T_InPlace_Double_OpMulScalar();

  // vector multiplication operators
  static final UFunc.UImpl2 MUL_DMDV = breeze.linalg.DenseMatrix$.MODULE$.implOpMulMatrix_DMD_DVD_eq_DVD();
  static final UFunc.UImpl2 MUL_DMSV = breeze.linalg.SparseVector.implOpMulMatrix_DM_SV_eq_DV_Double();
  static final UFunc.UImpl2 MUL_SMDV = breeze.linalg.CSCMatrix.canMulM_DV_Double();
  static final UFunc.UImpl2 MUL_SMSV = breeze.linalg.CSCMatrix.canMulM_SV_Double();

  // matrix multiplication operators
  static final UFunc.UImpl2 MUL_DMDM = breeze.linalg.DenseMatrix$.MODULE$.implOpMulMatrix_DMD_DMD_eq_DMD();
  static final UFunc.UImpl2 MUL_DMSM = breeze.linalg.CSCMatrix.canMulDM_M_Double();
  static final UFunc.UImpl2 MUL_SMDM = breeze.linalg.CSCMatrix.canMulM_DM_Double();
  static final UFunc.UImpl2 MUL_SMSM = breeze.linalg.CSCMatrix.canMulM_M_Double();

  // matrix element-wise multiplication operators
  static final UFunc.UImpl2 EMUL_DD = breeze.linalg.DenseMatrix.op_DM_DM_Double_OpMulScalar();
  static final UFunc.InPlaceImpl2 EMULI_DD = breeze.linalg.DenseMatrix.dm_dm_UpdateOp_Double_OpMulScalar();
  static final UFunc.UImpl2 EMUL_SS = breeze.linalg.CSCMatrix.csc_csc_OpMulScalar_Double();
  static final UFunc.InPlaceImpl2 EMULI_SS = breeze.linalg.CSCMatrix.csc_csc_InPlace_Double_OpMulScalar();
  static final UFunc.UImpl2 EMUL_MM = Matrix$.MODULE$.op_M_DM_Double_OpMulScalar();
  static final UFunc.InPlaceImpl2 EMULI_MM = Matrix$.MODULE$.m_m_UpdateOp_Double_OpMulScalar();

  // scalar division operators
  static final UFunc.UImpl2 DIV_DT = breeze.linalg.DenseMatrix.op_DM_S_Double_OpDiv();
  static final UFunc.InPlaceImpl2 DIVI_DT = breeze.linalg.DenseMatrix.dm_s_UpdateOp_Double_OpDiv();
  static final UFunc.UImpl2 DIV_TD = breeze.linalg.DenseMatrix.s_dm_op_Double_OpDiv();
  static final UFunc.UImpl2 DIV_ST = breeze.linalg.CSCMatrix.csc_T_Op_OpDiv(FIELD, TAG);
  static final UFunc.InPlaceImpl2 DIVI_ST = breeze.linalg.CSCMatrix.csc_T_InPlace_Double_OpDiv();

  // matrix element-wise division operators
  static final UFunc.UImpl2 EDIV_DD = breeze.linalg.DenseMatrix.op_DM_DM_Double_OpDiv();
  static final UFunc.InPlaceImpl2 EDIVI_DD = breeze.linalg.DenseMatrix.dm_dm_UpdateOp_Double_OpDiv();
  static final UFunc.UImpl2 EDIV_SS = breeze.linalg.CSCMatrix.csc_csc_BadOps_Double_OpDiv();
  static final UFunc.InPlaceImpl2 EDIVI_SS = breeze.linalg.CSCMatrix.csc_csc_InPlace_Double_OpDiv();
  static final UFunc.UImpl2 EDIV_MM = Matrix$.MODULE$.op_M_DM_Double_OpDiv();
  static final UFunc.InPlaceImpl2 EDIVI_MM = Matrix$.MODULE$.m_m_UpdateOp_Double_OpDiv();
}
