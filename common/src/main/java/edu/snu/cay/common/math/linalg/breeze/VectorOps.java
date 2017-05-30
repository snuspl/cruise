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
import breeze.linalg.support.CanSlice;

/**
 * Class for breeze vector operators.
 */
public final class VectorOps {

  private VectorOps() {
  }

  // set operators
  static final UFunc.InPlaceImpl2 SET_DD = breeze.linalg.DenseVector.dv_dv_UpdateOp_Float_OpSet();
  static final UFunc.InPlaceImpl2 SET_DS = breeze.linalg.DenseVector.dv_v_InPlaceOp_Float_OpSet();

  // slice operators
  static final CanSlice SLICE_D = breeze.linalg.DenseVector.canSlice();

  // scalar addition operator
  static final UFunc.UImpl2 ADD_DT = breeze.linalg.DenseVector.dv_s_Op_Float_OpAdd();
  static final UFunc.InPlaceImpl2 ADDI_DT = breeze.linalg.DenseVector.dv_s_UpdateOp_Float_OpAdd();
  static final UFunc.UImpl2 ADD_ST = breeze.linalg.SparseVector.implOps_SVT_T_eq_SVT_Float_OpAdd();
  static final UFunc.InPlaceImpl2 ADDI_ST = breeze.linalg.SparseVector.implOps_SVT_T_InPlace_Float_OpAdd();

  // vector addition operator
  static final UFunc.UImpl2 ADD_DD = breeze.linalg.DenseVector.canAddF();
  static final UFunc.InPlaceImpl2 ADDI_DD = breeze.linalg.DenseVector.canAddIntoF();
  static final UFunc.UImpl2 ADD_DS = breeze.linalg.SparseVector.dv_sv_op_Float_OpAdd();
  static final UFunc.InPlaceImpl2 ADDI_DS = breeze.linalg.SparseVector.implOps_DVT_SVT_InPlace_Float_OpAdd();
  static final UFunc.UImpl2 ADD_SS = breeze.linalg.SparseVector.implOps_SVT_SVT_eq_SVT_Float_OpAdd();

  // scalar subtraction operator
  static final UFunc.UImpl2 SUB_DT = breeze.linalg.DenseVector.dv_s_Op_Float_OpSub();
  static final UFunc.InPlaceImpl2 SUBI_DT = breeze.linalg.DenseVector.dv_s_UpdateOp_Float_OpSub();
  static final UFunc.UImpl2 SUB_ST = breeze.linalg.SparseVector.implOps_SVT_T_eq_SVT_Float_OpSub();
  static final UFunc.InPlaceImpl2 SUBI_ST = breeze.linalg.SparseVector.implOps_SVT_T_InPlace_Float_OpSub();

  // vector subtraction operator
  static final UFunc.UImpl2 SUB_DD = breeze.linalg.DenseVector.canSubF();
  static final UFunc.InPlaceImpl2 SUBI_DD = breeze.linalg.DenseVector.canSubIntoF();
  static final UFunc.UImpl2 SUB_DS = breeze.linalg.SparseVector.dv_sv_op_Float_OpSub();
  static final UFunc.InPlaceImpl2 SUBI_DS = breeze.linalg.SparseVector.implOps_DVT_SVT_InPlace_Float_OpSub();
  static final UFunc.UImpl2 SUB_SS = breeze.linalg.SparseVector.implOps_SVT_SVT_eq_SVT_Float_OpSub();

  // scale operator
  static final UFunc.UImpl2 SCALE_D = breeze.linalg.DenseVector.dv_s_Op_Float_OpMulMatrix();
  static final UFunc.InPlaceImpl2 SCALEI_D = breeze.linalg.DenseVector.dv_s_UpdateOp_Float_OpMulMatrix();
  static final UFunc.UImpl2 SCALE_S = breeze.linalg.SparseVector.implOps_SVT_T_eq_SVT_Float_OpMulMatrix();

  // division operator
  static final UFunc.UImpl2 DIV_D = breeze.linalg.DenseVector.dv_s_Op_Float_OpDiv();
  static final UFunc.InPlaceImpl2 DIVI_D = breeze.linalg.DenseVector.dv_s_UpdateOp_Float_OpDiv();
  static final UFunc.UImpl2 DIV_S = breeze.linalg.SparseVector.implOps_SVT_T_eq_SVT_Float_OpDiv();
  static final UFunc.InPlaceImpl2 DIVI_S = breeze.linalg.SparseVector.implOps_SVT_T_InPlace_Float_OpDiv();

  // axpy operator
  static final UFunc.InPlaceImpl3 AXPY_DD = breeze.linalg.DenseVector.axpy_Float();
  static final UFunc.InPlaceImpl3 AXPY_DS = breeze.linalg.SparseVector.implScaleAdd_DVT_T_SVT_InPlace_Float();
  static final UFunc.InPlaceImpl3 AXPY_SS = breeze.linalg.SparseVector.implScaleAdd_SVT_T_SVT_InPlace_Float();

  // dot operator
  static final UFunc.UImpl2 DOT_DD = breeze.linalg.DenseVector$.MODULE$.canDot_DV_DV_Float();
  static final UFunc.UImpl2 DOT_DS = breeze.linalg.SparseVector.implOpMulInner_DVT_SVT_eq_T_Float();
  static final UFunc.UImpl2 DOT_SS = SparseVector.implOpMulInner_SVT_SVT_eq_T_Float();

}
