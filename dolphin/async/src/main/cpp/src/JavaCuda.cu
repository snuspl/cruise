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
#include <thrust/device_ptr.h>
#include <thrust/equal.h>
#include <thrust/reduce.h>

#include <stdexcept>
#include <utility>
#include <cfloat>
#include <cmath>

#include "JavaCuda.h"

#define CUDA_KERNEL_LOOP(i, n) \
  for (int i = blockIdx.x * blockDim.x + threadIdx.x; \
       i < (n); \
       i += blockDim.x * gridDim.x)

struct float_compare {
  float tolerance;
  float_compare(const float t) : tolerance(t) {
  }
  __device__ bool operator()(const float x, const float y) const {
    return abs(x - y) < tolerance;
  }
};

std::pair<cublasOperation_t, bool> getCublasOperation(const char c) {
  switch (c) {
  case 'N':
  case 'n':
    return std::make_pair(CUBLAS_OP_N, true);
  case 'T':
  case 't':
    return std::make_pair(CUBLAS_OP_T, true);
  case 'C':
  case 'c':
    return std::make_pair(CUBLAS_OP_C, true);
  default:
    return std::make_pair(CUBLAS_OP_N, false);
  }
}

void freeCublasHandle(cublasHandle_t* handle) {
  if (*handle != NULL) {
    cublasDestroy(*handle);
  }
  delete handle;
}

boost::thread_specific_ptr<cublasHandle_t> JavaCuda::cublasHandle(freeCublasHandle);

/*
 * Get cuBLAS handle which is required for every functions in cuBLAS.
 * All threads have different cuBLAS handle pointer by using boost thread specific pointer.
 * When a thread requests for cuBLAS handle, it checks whether thread specific pointer for cuBLAS handle is set.
 * If not, new cuBLAS handle is created.
 * Thread specific pointer is destroyed by freeCublasHandle().
 * This destroying is automatically done by boost when the thread is killed.
 */
cublasHandle_t JavaCuda::getCublasHandle() {
  if (!cublasHandle.get()) {
    // allocate new cublas handle
    cublasHandle_t* newHandlePtr = new cublasHandle_t;
    if (CUBLAS_STATUS_SUCCESS != cublasCreate(newHandlePtr)) {
      throw std::runtime_error("Cannot create Cublas handle. Cublas won't be available.");
    }
    cublasHandle.reset(newHandlePtr);
  }
  return *cublasHandle.get();
}

void* JavaCuda::cudaDeviceMalloc(size_t size) {
  void* devPtr;
  if (cudaSuccess != cudaMalloc(&devPtr, size)) {
    devPtr = NULL;
  }
  return devPtr;
}

bool JavaCuda::cudaDeviceFree(void* devPtr) {
  return cudaSuccess == cudaFree(devPtr);
}

bool JavaCuda::d2hMemcpy(void* dst, const void* src, const int n) {
  return cudaSuccess == cudaMemcpy(dst, src, n, cudaMemcpyDeviceToHost);
}

bool JavaCuda::h2dMemcpy(void* dst, const void* src, const int n) {
  return cudaSuccess == cudaMemcpy(dst, src, n, cudaMemcpyHostToDevice);
}

bool JavaCuda::d2dMemcpy(void* dst, const void* src, const int n) {
  return cudaSuccess == cudaMemcpy(dst, src, n, cudaMemcpyDeviceToDevice);
}

bool JavaCuda::d2dMemcpy2D(void* dst, const int dpitch, const void* src, const int spitch, const int length, const int n) {
  return cudaSuccess == cudaMemcpy2D(dst, dpitch, src, spitch, length, n, cudaMemcpyDeviceToDevice);
}

__global__ void set_kernel(const int n, const float a, float* y) {
  CUDA_KERNEL_LOOP(i, n) {
    y[i] = a;
  }
}

bool JavaCuda::set(float* y, const float a, const int n) {
  if (a == 0) {
    return cudaSuccess == cudaMemset(y, 0, sizeof(float) * n);
  }

  set_kernel<<<GET_BLOCKS(n), CUDA_NUM_THREADS>>>(n, a, y);
  return true;
}

bool JavaCuda::copy(const int n, const float* x, int incx, float* y, int incy) {
  return CUBLAS_STATUS_SUCCESS == cublasScopy(getCublasHandle(), n, x, incx, y, incy);
}

bool JavaCuda::compare(const int n, const float* x, const float* y, const float tolerance) {
  thrust::device_ptr<float> device_ptr_x(const_cast<float*>(x));
  thrust::device_ptr<float> device_ptr_y(const_cast<float*>(y));
  return thrust::equal(device_ptr_x, device_ptr_x + n, device_ptr_y, float_compare(tolerance));
}

float JavaCuda::sum(const int n, const float* x) {
  thrust::device_ptr<float> device_ptr(const_cast<float*>(x));
  return thrust::reduce(device_ptr, device_ptr + n, 0.0f, thrust::plus<float>());
}

float JavaCuda::max(const int n, const float* x) {
  thrust::device_ptr<float> device_ptr(const_cast<float*>(x));
  return thrust::reduce(device_ptr, device_ptr + n, FLT_MIN, thrust::maximum<float>());
}

float JavaCuda::min(const int n, const float* x) {
  thrust::device_ptr<float> device_ptr(const_cast<float*>(x));
  return thrust::reduce(device_ptr, device_ptr + n, FLT_MAX, thrust::minimum<float>());
}

__global__ void column_max_kernel(const int m, const int n, const float* x, float* y) {
  CUDA_KERNEL_LOOP(i, n) {
    float max = FLT_MIN;
    for (int j = m * i; j < m * (i + 1); ++j) {
      max = max < x[j] ? x[j] : max;
    }
    y[i] = max;
  }
}

bool JavaCuda::columnMax(const int m, const int n, const float* x, float* y) {
  column_max_kernel<<<GET_BLOCKS(n), CUDA_NUM_THREADS>>>(m, n, x, y);
  return true;
}

__global__ void row_max_kernel(const int m, const int n, const float* x, float* y) {
  CUDA_KERNEL_LOOP(i, m) {
    float max = FLT_MIN;
    for (int j = i; j < m * n; j += m) {
      max = max < x[j] ? x[j] : max;
    }
    y[i] = max;
  }
}

bool JavaCuda::rowMax(const int m, const int n, const float* x, float* y) {
  row_max_kernel<<<GET_BLOCKS(m), CUDA_NUM_THREADS>>>(m, n, x, y);
  return true;
}

__global__ void column_min_kernel(const int m, const int n, const float* x, float* y) {
  CUDA_KERNEL_LOOP(i, n) {
    float min = FLT_MAX;
    for (int j = m * i; j < m * (i + 1); ++j) {
      min = min > x[j] ? x[j] : min;
    }
    y[i] = min;
  }
}

bool JavaCuda::columnMin(const int m, const int n, const float* x, float* y) {
  column_min_kernel<<<GET_BLOCKS(n), CUDA_NUM_THREADS>>>(m, n, x, y);
  return true;
}

__global__ void row_min_kernel(const int m, const int n, const float* x, float* y) {
  CUDA_KERNEL_LOOP(i, m) {
    float min = FLT_MAX;
    for (int j = i; j < m * n; j += m) {
      min = min > x[j] ? x[j] : min;
    }
    y[i] = min;
  }
}

bool JavaCuda::rowMin(const int m, const int n, const float* x, float* y) {
  row_min_kernel<<<GET_BLOCKS(m), CUDA_NUM_THREADS>>>(m, n, x, y);
  return true;
}

__global__ void add_scalar_kernel(const int n, const float a, const float* x, float* y) {
  CUDA_KERNEL_LOOP(i, n) {
    y[i] = x[i] + a;
  }
}

bool JavaCuda::addScalar(const int n, const float a, const float* x, float* y) {
  add_scalar_kernel<<<GET_BLOCKS(n), CUDA_NUM_THREADS>>>(n, a, x, y);
  return true;
}

bool JavaCuda::mulScalar(const int n, const float a, const float* x, float* y) {
  if (x != y) {
    if (!JavaCuda::d2dMemcpy(y, x, n)) {
      return false;
    }
  }
  return CUBLAS_STATUS_SUCCESS == cublasSscal(getCublasHandle(), n, &a, y, 1);
}

__global__ void rsub_scalar_kernel(const int n, const float a, const float* x, float* y) {
  CUDA_KERNEL_LOOP(i, n) {
    y[i] = a - x[i];
  }
}

bool JavaCuda::rsubScalar(const int n, const float a, const float* x, float* y) {
  rsub_scalar_kernel<<<GET_BLOCKS(n), CUDA_NUM_THREADS>>>(n, a, x, y);
  return true;
}

__global__ void rdiv_scalar_kernel(const int n, const float a, const float* x, float* y) {
  CUDA_KERNEL_LOOP(i, n) {
    y[i] = a / x[i];
  }
}

bool JavaCuda::rdivScalar(const int n, const float a, const float* x, float* y) {
  rdiv_scalar_kernel<<<GET_BLOCKS(n), CUDA_NUM_THREADS>>>(n, a, x, y);
  return true;
}

__global__ void mul_column_vector_kernel(const int m, const int n, const float* v, float* x, float* y) {
  CUDA_KERNEL_LOOP(i, m * n) {
    y[i] = x[i] * v[i % m];
  }
}

bool JavaCuda::mulColumnVector(const int m, const int n, const float* v, float* x, float* y) {
  mul_column_vector_kernel<<<GET_BLOCKS(m * n), CUDA_NUM_THREADS>>>(m, n, v, x, y);
  return true;
}

__global__ void mul_row_vector_kernel(const int m, const int n, const float* v, float* x, float* y) {
  CUDA_KERNEL_LOOP(i, m * n) {
    y[i] = x[i] * v[i / m];
  }
}

bool JavaCuda::mulRowVector(const int m, const int n, const float* v, float* x, float* y) {
  mul_row_vector_kernel<<<GET_BLOCKS(m * n), CUDA_NUM_THREADS>>>(m, n, v, x, y);
  return true;
}

__global__ void div_column_vector_kernel(const int m, const int n, const float* v, float* x, float* y) {
  CUDA_KERNEL_LOOP(i, m * n) {
    y[i] = x[i] / v[i % m];
  }
}

bool JavaCuda::divColumnVector(const int m, const int n, const float* v, float* x, float* y) {
  div_column_vector_kernel<<<GET_BLOCKS(m * n), CUDA_NUM_THREADS>>>(m, n, v, x, y);
  return true;
}

__global__ void div_row_vector_kernel(const int m, const int n, const float* v, float* x, float* y) {
  CUDA_KERNEL_LOOP(i, m * n) {
    y[i] = x[i] / v[i / m];
  }
}

bool JavaCuda::divRowVector(const int m, const int n, const float* v, float* x, float* y) {
  div_row_vector_kernel<<<GET_BLOCKS(m * n), CUDA_NUM_THREADS>>>(m, n, v, x, y);
  return true;
}

__global__ void add_kernel(const int n, const float* a, const float* b, float* y) {
  CUDA_KERNEL_LOOP(i, n) {
    y[i] = a[i] + b[i];
  }
}

bool JavaCuda::add(const int n, const float* a, const float* b, float* y) {
  add_kernel<<<GET_BLOCKS(n), CUDA_NUM_THREADS>>>(n, a, b, y);
  return true;
}

__global__ void sub_kernel(const int n, const float* a, const float* b, float* y) {
  CUDA_KERNEL_LOOP(i, n) {
    y[i] = a[i] - b[i];
  }
}

bool JavaCuda::sub(const int n, const float* a, const float* b, float* y) {
  sub_kernel<<<GET_BLOCKS(n), CUDA_NUM_THREADS>>>(n, a, b, y);
  return true;
}

__global__ void mul_kernel(const int n, const float* a, const float* b, float* y) {
  CUDA_KERNEL_LOOP(i, n) {
    y[i] = a[i] * b[i];
  }
}

bool JavaCuda::mul(const int n, const float* a, const float* b, float* y) {
  mul_kernel<<<GET_BLOCKS(n), CUDA_NUM_THREADS>>>(n, a, b, y);
  return true;
}

__global__ void div_kernel(const int n, const float* a, const float* b, float* y) {
  CUDA_KERNEL_LOOP(i, n) {
    y[i] = a[i] / b[i];
  }
}

bool JavaCuda::div(const int n, const float* a, const float* b, float* y) {
  div_kernel<<<GET_BLOCKS(n), CUDA_NUM_THREADS>>>(n, a, b, y);
  return true;
}

bool JavaCuda::geam(const char transa, const char transb, const int m, const int n,
                    const float alpha, const float* a, const int lda,
                    const float beta, const float* b, const int ldb,
                    float* c, const int ldc) {
  std::pair<cublasOperation_t, bool> opa = getCublasOperation(transa);
  std::pair<cublasOperation_t, bool> opb = getCublasOperation(transb);
  if (!opa.second || !opb.second) {
    return false;
  }
  return CUBLAS_STATUS_SUCCESS == cublasSgeam(getCublasHandle(), opa.first, opb.first, m, n, &alpha, a, lda, &beta, b, ldb, c, ldc);
}

bool JavaCuda::gemv(const char trans, const int m, const int n,
                    const float alpha, const float* a, const int lda,
                    const float* x, const int incx,
                    const float beta, float* y, const int incy) {
  std::pair<cublasOperation_t, bool> op = getCublasOperation(trans);
  if (!op.second) {
    return false;
  }
  return CUBLAS_STATUS_SUCCESS == cublasSgemv(getCublasHandle(), op.first, m, n, &alpha, a, lda, x, incx, &beta, y, incy);
}

bool JavaCuda::gemm(const char transa, const char transb, const int m, const int n, const int k,
                    const float alpha, const float* a, const int lda,
                    const float* b, const int ldb,
                    const float beta, float* c, const int ldc) {
  std::pair<cublasOperation_t, bool> opa = getCublasOperation(transa);
  std::pair<cublasOperation_t, bool> opb = getCublasOperation(transb);
  if (!opa.second || !opb.second) {
    return false;
  }
  return CUBLAS_STATUS_SUCCESS == cublasSgemm(getCublasHandle(), opa.first, opb.first, m, n, k, &alpha, a, lda, b, ldb, &beta, c, ldc);
}
