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
import breeze.linalg.support.CanTranspose;
import breeze.math.Semiring;
import breeze.math.Semiring$;
import breeze.storage.Zero;
import breeze.storage.Zero$;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 * Class for breeze matrix operators.
 */
public final class MatrixOps {

  private static final ClassTag TAG = ClassTag$.MODULE$.Double();
  private static final Zero ZERO = Zero$.MODULE$.forClass(Double.TYPE);
  private static final Semiring RING = Semiring$.MODULE$.semiringD();

  private MatrixOps() {
  }

  // transpose operators
  static final CanTranspose T_D = breeze.linalg.DenseMatrix.canTranspose();
  static final CanTranspose T_S = breeze.linalg.CSCMatrix.canTranspose(TAG, ZERO, RING);

  // add operators
  static final UFunc.UImpl2 ADD_DD = breeze.linalg.DenseMatrix.op_DM_DM_Double_OpAdd();
  static final UFunc.InPlaceImpl2 ADDI_DD = breeze.linalg.DenseMatrix.dm_dm_UpdateOp_Double_OpAdd();
  static final UFunc.UImpl2 ADD_DS = breeze.linalg.CSCMatrix.dm_csc_OpAdd_Double();
  static final UFunc.InPlaceImpl2 ADDI_DS = breeze.linalg.CSCMatrix.dm_csc_InPlace_OpAdd_Double();
  static final UFunc.UImpl2 ADD_SS = breeze.linalg.CSCMatrix.csc_csc_OpAdd_Double();
  static final UFunc.InPlaceImpl2 ADDI_SS = breeze.linalg.CSCMatrix.csc_csc_InPlace_Double_OpAdd();

  // subtract operators
  static final UFunc.UImpl2 SUB_DD = breeze.linalg.DenseMatrix.op_DM_DM_Double_OpSub();
  static final UFunc.InPlaceImpl2 SUBI_DD = breeze.linalg.DenseMatrix.dm_dm_UpdateOp_Double_OpSub();
  static final UFunc.UImpl2 SUB_DS = breeze.linalg.CSCMatrix.dm_csc_OpSub_Double();
  static final UFunc.InPlaceImpl2 SUBI_DS = breeze.linalg.CSCMatrix.dm_csc_InPlace_OpSub_Double();
  static final UFunc.UImpl2 SUB_SS = breeze.linalg.CSCMatrix.csc_csc_OpSub_Double();
  static final UFunc.InPlaceImpl2 SUBI_SS = breeze.linalg.CSCMatrix.csc_csc_InPlace_Double_OpSub();

  // scalar multiplication operatros
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
}
