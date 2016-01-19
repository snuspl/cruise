/*
 * Copyright (C) 2015 Seoul National University
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
import breeze.linalg.SparseVector;

/**
 * Class for breeze vector operators.
 */
public final class VectorOps {

  private VectorOps() {
  }

  // add operator
  static final UFunc.UImpl2 ADD_DD = breeze.linalg.DenseVector.canAddD();
  static final UFunc.InPlaceImpl2 ADDI_DD = breeze.linalg.DenseVector.canAddIntoD();
  static final UFunc.UImpl2 ADD_DS = breeze.linalg.SparseVector.dv_sv_op_Double_OpAdd();
  static final UFunc.InPlaceImpl2 ADDI_DS = breeze.linalg.SparseVector.implOps_DVT_SVT_InPlace_Double_OpAdd();
  static final UFunc.UImpl2 ADD_SS = breeze.linalg.SparseVector.implOps_SVT_SVT_eq_SVT_Double_OpAdd();

  // subtract operator
  static final UFunc.UImpl2 SUB_DD = breeze.linalg.DenseVector.canSubD();
  static final UFunc.InPlaceImpl2 SUBI_DD = breeze.linalg.DenseVector.canSubIntoD();
  static final UFunc.UImpl2 SUB_DS = breeze.linalg.SparseVector.dv_sv_op_Double_OpSub();
  static final UFunc.InPlaceImpl2 SUBI_DS = breeze.linalg.SparseVector.implOps_DVT_SVT_InPlace_Double_OpSub();
  static final UFunc.UImpl2 SUB_SS = breeze.linalg.SparseVector.implOps_SVT_SVT_eq_SVT_Double_OpSub();

  // scale operator
  static final UFunc.UImpl2 SCALE_D = breeze.linalg.DenseVector.dv_s_Op_Double_OpMulMatrix();
  static final UFunc.InPlaceImpl2 SCALEI_D = breeze.linalg.DenseVector.dv_s_UpdateOp_Double_OpMulMatrix();
  static final UFunc.UImpl2 SCALE_S = breeze.linalg.SparseVector.implOps_SVT_T_eq_SVT_Double_OpMulMatrix();

  // axpy operator
  static final UFunc.InPlaceImpl3 AXPY_DD = breeze.linalg.DenseVector.canDaxpy$.MODULE$;
  static final UFunc.InPlaceImpl3 AXPY_DS = breeze.linalg.SparseVector.implScaleAdd_DVT_T_SVT_InPlace_Double();
  static final UFunc.InPlaceImpl3 AXPY_SS = breeze.linalg.SparseVector.implScaleAdd_SVT_T_SVT_InPlace_Double();

  // dot operator
  static final UFunc.UImpl2 DOT_DD = breeze.linalg.DenseVector.canDotD$.MODULE$;
  static final UFunc.UImpl2 DOT_DS = breeze.linalg.SparseVector.implOpMulInner_DVT_SVT_eq_T_Double();
  static final UFunc.UImpl2 DOT_SS = SparseVector.implOpMulInner_SVT_SVT_eq_T_Double();

}
