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
package edu.snu.cay.dolphin.async.dnn.blas.cuda;

import org.bytedeco.javacpp.FloatPointer;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.Pointer;
import org.bytedeco.javacpp.annotation.Cast;
import org.bytedeco.javacpp.annotation.Platform;

/**
 * Java-Cuda JNI wrapper.
 */
@Platform(include = "JavaCuda.h", link = {"boost_thread", "cudart", "cublas", "javacuda"})
class JavaCuda extends Pointer {
  static {
    Loader.load();
  }

  JavaCuda() {
    allocate();
  }
  private native void allocate();

  @Cast(value = "void*") static native Pointer deviceMalloc(@Cast(value = "size_t") final long size);
  @Cast(value = "bool") static native boolean deviceFree(final Pointer devPtr);
  @Cast(value = "bool") static native boolean set(final FloatPointer y, final float a, final int n);
  @Cast(value = "bool") static native boolean d2hMemcpy(final Pointer dst, final Pointer src, final int n);
  @Cast(value = "bool") static native boolean h2dMemcpy(final Pointer dst, final Pointer src, final int n);
  @Cast(value = "bool") static native boolean d2dMemcpy(final Pointer dst, final Pointer src, final int n);
  @Cast(value = "bool") static native boolean d2dMemcpy2D(final Pointer dst, final int dpitch, final Pointer src,
                                                          final int spitch, final int length, final int n);
  @Cast(value = "bool") static native boolean copy(final int n, final FloatPointer x, final int incx,
                                                   final FloatPointer y, final int incy);
  @Cast(value = "bool") static native boolean compare(final int n, final FloatPointer x,
                                                      final FloatPointer y, final float tolerance);

  static native float sum(final int n, final FloatPointer x);
  static native float max(final int n, final FloatPointer x);
  static native float min(final int n, final FloatPointer x);

  @Cast(value = "bool") static native boolean columnMax(final int m, final int n,
                                                        final FloatPointer x, final FloatPointer y);
  @Cast(value = "bool") static native boolean rowMax(final int m, final int n,
                                                     final FloatPointer x, final FloatPointer y);
  @Cast(value = "bool") static native boolean columnMin(final int m, final int n,
                                                        final FloatPointer x, final FloatPointer y);
  @Cast(value = "bool") static native boolean rowMin(final int m, final int n,
                                                     final FloatPointer x, final FloatPointer y);

  @Cast(value = "bool") static native boolean addScalar(final int n, final float a,
                                                        final FloatPointer x, final FloatPointer y);
  @Cast(value = "bool") static native boolean mulScalar(final int n, final float a,
                                                        final FloatPointer x, final FloatPointer y);
  @Cast(value = "bool") static native boolean rsubScalar(final int n, final float a,
                                                         final FloatPointer x, final FloatPointer y);
  @Cast(value = "bool") static native boolean rdivScalar(final int n, final float a,
                                                         final FloatPointer x, final FloatPointer y);

  @Cast(value = "bool") static native boolean mulColumnVector(final int m, final int n, final FloatPointer v,
                                                              final FloatPointer x, final FloatPointer y);
  @Cast(value = "bool") static native boolean mulRowVector(final int m, final int n, final FloatPointer v,
                                                           final FloatPointer x, final FloatPointer y);
  @Cast(value = "bool") static native boolean divColumnVector(final int m, final int n, final FloatPointer v,
                                                              final FloatPointer x, final FloatPointer y);
  @Cast(value = "bool") static native boolean divRowVector(final int m, final int n, final FloatPointer v,
                                                           final FloatPointer x, final FloatPointer y);

  @Cast(value = "bool") static native boolean add(final int n, final FloatPointer a,
                                                  final FloatPointer b, final FloatPointer y);
  @Cast(value = "bool") static native boolean sub(final int n, final FloatPointer a,
                                                  final FloatPointer b, final FloatPointer y);
  @Cast(value = "bool") static native boolean mul(final int n, final FloatPointer a,
                                                  final FloatPointer b, final FloatPointer y);
  @Cast(value = "bool") static native boolean div(final int n, final FloatPointer a,
                                                  final FloatPointer b, final FloatPointer y);

  @Cast(value = "bool") static native boolean geam(final char transa, final char transb, final int m, final int n,
                                                   final float alpha, final FloatPointer a, final int lda,
                                                   final float beta, final FloatPointer b, final int ldb,
                                                   final FloatPointer c, final int ldc);
  @Cast(value = "bool") static native boolean gemv(final char trans, final int m, final int n,
                                                   final float alpha, final FloatPointer a, final int lda,
                                                   final FloatPointer x, final int incx,
                                                   final float beta, final FloatPointer y, final int incy);
  @Cast(value = "bool") static native boolean gemm(final char transa, final char transb,
                                                   final int m, final int n, final int k,
                                                   final float alpha, final FloatPointer a, final int lda,
                                                   final FloatPointer b, final int ldb, final float beta,
                                                   final FloatPointer c, final int ldc);
}
