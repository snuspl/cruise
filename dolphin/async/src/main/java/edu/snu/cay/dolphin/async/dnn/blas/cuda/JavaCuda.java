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
 * Needs inclusion and linkage of following file & libraries.
 * Note that the linkage order may be crucial: javacuda -> cublas -> cudart -> boost_thread
 */
@Platform(include = "JavaCuda.h", link = {"boost_thread", "cudart", "cublas", "javacuda"})
public class JavaCuda extends Pointer {
  static {
    Loader.load();
  }

  JavaCuda() {
    allocate();
  }

  private native void allocate();

  @Cast(value = "void*")
  public static native Pointer cudaDeviceMalloc(@Cast(value = "size_t") long size);

  public static Pointer deviceMalloc(final long size) {
    if (size == 0) {
      return new Pointer();
    }

    final Pointer pointer = new Pointer(cudaDeviceMalloc(size));
    if (pointer.isNull()) {
      throw new RuntimeException("Allocation cuda device memory is failed");
    }
    return pointer;
  }

  @Cast(value = "bool")
  public static native boolean cudaDeviceFree(Pointer devPtr);

  public static void deviceFree(final Pointer pointer) {
    final boolean success = cudaDeviceFree(pointer);

    if (!success) {
      throw new RuntimeException("Cuda device memory is failed to free");
    }
  }

  @Cast(value = "bool")
  public static native boolean set(FloatPointer y, float a, int n);

  @Cast(value = "bool")
  public static native boolean d2hMemcpy(Pointer dst, Pointer src, int n);

  @Cast(value = "bool")
  public static native boolean h2dMemcpy(Pointer dst, Pointer src, int n);

  @Cast(value = "bool")
  public static native boolean d2dMemcpy(Pointer dst, Pointer src, int n);

  @Cast(value = "bool")
  public static native boolean d2dMemcpy2D(Pointer dst, int dpitch, Pointer src,
                                           int spitch, int length, int n);

  @Cast(value = "bool")
  public static native boolean copy(int n, FloatPointer x, int incx,
                                    FloatPointer y, int incy);

  @Cast(value = "bool")
  public static native boolean compare(int n, FloatPointer x,
                                       FloatPointer y, float tolerance);

  @Cast(value = "bool")
  public static native boolean equal(int n, FloatPointer x,
                                     FloatPointer y);

  public static native float sum(int n, FloatPointer x);

  public static native float max(int n, FloatPointer x);

  public static native float min(int n, FloatPointer x);

  @Cast(value = "bool")
  public static native boolean columnMax(int m, int n,
                                         FloatPointer x, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean rowMax(int m, int n,
                                      FloatPointer x, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean columnMin(int m, int n,
                                         FloatPointer x, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean rowMin(int m, int n,
                                      FloatPointer x, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean addScalar(int n, float a,
                                         FloatPointer x, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean mulScalar(int n, float a,
                                         FloatPointer x, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean rsubScalar(int n, float a,
                                          FloatPointer x, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean rdivScalar(int n, float a,
                                          FloatPointer x, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean mulColumnVector(int m, int n, FloatPointer v,
                                               FloatPointer x, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean mulRowVector(int m, int n, FloatPointer v,
                                            FloatPointer x, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean divColumnVector(int m, int n, FloatPointer v,
                                               FloatPointer x, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean divRowVector(int m, int n, FloatPointer v,
                                            FloatPointer x, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean add(int n, FloatPointer a,
                                   FloatPointer b, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean sub(int n, FloatPointer a,
                                   FloatPointer b, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean mul(int n, FloatPointer a,
                                   FloatPointer b, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean div(int n, FloatPointer a,
                                   FloatPointer b, FloatPointer y);

  @Cast(value = "bool")
  public static native boolean geam(char transa, char transb, int m, int n,
                                    float alpha, FloatPointer a, int lda,
                                    float beta, FloatPointer b, int ldb,
                                    FloatPointer c, int ldc);

  @Cast(value = "bool")
  public static native boolean gemv(char trans, int m, int n,
                                    float alpha, FloatPointer a, int lda,
                                    FloatPointer x, int incx,
                                    float beta, FloatPointer y, int incy);

  @Cast(value = "bool")
  public static native boolean gemm(char transa, char transb,
                                    int m, int n, int k,
                                    float alpha, FloatPointer a, int lda,
                                    FloatPointer b, int ldb, float beta,
                                    FloatPointer c, int ldc);
}
