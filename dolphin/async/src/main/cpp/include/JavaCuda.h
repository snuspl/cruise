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
#ifndef __JAVA_CUDA_H__
#define __JAVA_CUDA_H__

#include <cublas_v2.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include <boost/thread.hpp>

/*
 * Native methods for JavaCuda.
 * Please refer to followings:
 * http://docs.nvidia.com/cuda/cuda-runtime-api
 * http://docs.nvidia.com/cuda/cublas
 * http://docs.nvidia.com/cuda/thrust
 */
class JavaCuda {
public:
  static void* cudaDeviceMalloc(size_t size);
  static bool cudaDeviceFree(void* devPtr);
  static bool d2hMemcpy(void* dst, const void* src, const int n);
  static bool h2dMemcpy(void* dst, const void* src, const int n);
  static bool d2dMemcpy(void* dst, const void* src, const int n);
  static bool d2dMemcpy2D(void* dst, const int dpitch, const void* src, const int spitch, const int length, const int n);
  static bool set(float* y, const float a, const int n);
  static bool copy(const int n, const float* x, int incx, float* y, int incy);
  static bool compare(const int n, const float* x, const float* y, const float tolerance);

  static float sum(const int n, const float* x);
  static float max(const int n, const float* x);
  static float min(const int n, const float* x);

  static bool columnMax(const int m, const int n, const float* x, float* y);
  static bool rowMax(const int m, const int n, const float* x, float* y);
  static bool columnMin(const int m, const int n, const float* x, float* y);
  static bool rowMin(const int m, const int n, const float* x, float* y);

  static bool addScalar(const int n, const float a, const float* x, float* y);
  static bool mulScalar(const int n, const float a, const float* x, float* y);
  static bool rsubScalar(const int n, const float a, const float* x, float* y);
  static bool rdivScalar(const int n, const float a, const float* x, float* y);

  static bool mulColumnVector(const int m, const int n, const float* v, float* x, float* y);
  static bool mulRowVector(const int m, const int n, const float* v, float* x, float* y);
  static bool divColumnVector(const int m, const int n, const float* v, float* x, float* y);
  static bool divRowVector(const int m, const int n, const float* v, float* x, float* y);

  static bool add(const int n, const float* a, const float* b, float* y);
  static bool sub(const int n, const float* a, const float* b, float* y);
  static bool mul(const int n, const float* a, const float* b, float* y);
  static bool div(const int n, const float* a, const float* b, float* y);

  static bool geam(const char transa, const char transb, const int m, const int n,
                   const float alpha, const float* a, const int lda,
                   const float beta, const float* b, const int ldb,
                   float* c, const int ldc);
  static bool gemv(const char trans, const int m, const int n,
                   const float alpha, const float* a, const int lda,
                   const float* x, const int incx,
                   const float beta, float* y, int incy);
  static bool gemm(const char transa, const char transb, const int m, const int n, const int k,
                   const float alpha, const float* a, const int lda,
                   const float* b, const int ldb,
                   const float beta, float* c, const int ldc);

private:
  static cublasHandle_t getCublasHandle();

private:
  static boost::thread_specific_ptr<cublasHandle_t> cublasHandle;
};

const int CUDA_NUM_THREADS = 512;

inline int GET_BLOCKS(const int N) {
  return (N + CUDA_NUM_THREADS - 1) / CUDA_NUM_THREADS;
}

#endif
