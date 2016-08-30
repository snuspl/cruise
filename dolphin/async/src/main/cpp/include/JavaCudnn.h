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
#ifndef __JAVA_CUDNN_H__
#define __JAVA_CUDNN_H__

#include <cuda.h>
#include <cuda_runtime.h>
#include <boost/thread.hpp>
#include <cudnn.h>

class JavaCudnn {
public:
  static cudnnTensorDescriptor_t* createTensorDesc(const int n, const int c, const int h, const int w,
                                const int nStride, const int cStride, const int hStride, const int wStride);
  static cudnnTensorDescriptor_t* createTensorDesc(const int n, const int c, const int h, const int w);
  static cudnnFilterDescriptor_t* createFilterDesc(const int k, const int c, const int h, const int w);
  static cudnnConvolutionDescriptor_t* createConvDesc(const int padH, const int padW, const int strideH, const int strideW);
  static cudnnPoolingDescriptor_t* createPoolDesc(const int mode, const int h, const int w, const int padH, const int padW, const int strideH, const int strideW);
  static cudnnActivationDescriptor_t* createActivDesc(const int fun);
  static cudnnLRNDescriptor_t* createLRNDesc(const int localSize, const float alpha, const float beta, const float k);


  static void* getWorkspace (const size_t workspaceSizeInBytes);

  static cudnnConvolutionFwdAlgo_t* getConvForwardAlgo(const cudnnTensorDescriptor_t* xDesc, const cudnnFilterDescriptor_t* wDesc,
                                                       const cudnnConvolutionDescriptor_t* convDesc, const cudnnTensorDescriptor_t* yDesc);
  static cudnnConvolutionBwdDataAlgo_t* getConvBackwardDataAlgo(const cudnnFilterDescriptor_t* wDesc, const cudnnTensorDescriptor_t* dyDesc,
                                                                const cudnnConvolutionDescriptor_t* convDesc, const cudnnTensorDescriptor_t* dxDesc);
  static cudnnConvolutionBwdFilterAlgo_t* getConvBackwardFilterAlgo(const cudnnTensorDescriptor_t* xDesc, const cudnnTensorDescriptor_t* dyDesc,
                                                                    const cudnnConvolutionDescriptor_t* convDesc, const cudnnFilterDescriptor_t* dwDesc);

  static size_t getConvForwardWorkspaceSizeInBytes(const cudnnTensorDescriptor_t* xDesc, const cudnnFilterDescriptor_t* wDesc,
                                                   const cudnnConvolutionDescriptor_t* convDesc, const cudnnTensorDescriptor_t* yDesc,
                                                   const cudnnConvolutionFwdAlgo_t* algo);
  static size_t getConvBackwardDataWorkspaceSizeInBytes(const cudnnFilterDescriptor_t* wDesc, const cudnnTensorDescriptor_t* dyDesc,
                                                        const cudnnConvolutionDescriptor_t* convDesc, const cudnnTensorDescriptor_t* dxDesc,
                                                        const cudnnConvolutionBwdDataAlgo_t* algo);
  static size_t getConvBackwardFilterWorkspaceSizeInBytes(const cudnnTensorDescriptor_t* xDesc, const cudnnTensorDescriptor_t* dyDesc,
                                                          const cudnnConvolutionDescriptor_t* convDesc, const cudnnFilterDescriptor_t* dwDesc,
                                                          const cudnnConvolutionBwdFilterAlgo_t* algo);

  static bool convFeedForward(const cudnnTensorDescriptor_t* xDesc, const void* x,
                              const cudnnFilterDescriptor_t* wDesc, const void* w,
                              const cudnnTensorDescriptor_t* bDesc, const void* b,
                              const cudnnConvolutionDescriptor_t* convDesc, const cudnnConvolutionFwdAlgo_t* algo,
                              void* workspace, size_t workspaceSizeInBytes,
                              const cudnnTensorDescriptor_t* yDesc, void* y);
  static bool convBackPropagate(const cudnnFilterDescriptor_t* wDesc, const void* w,
                                const cudnnTensorDescriptor_t* dyDesc, const void* dy,
                                const cudnnConvolutionDescriptor_t* convDesc, const cudnnConvolutionBwdDataAlgo_t* algo,
                                void* workspace, size_t workspaceSizeInBytes,
                                const cudnnTensorDescriptor_t* dxDesc, void* dx);
  static bool convGenWeightGradient(const cudnnTensorDescriptor_t* xDesc, const void* x,
                                    const cudnnTensorDescriptor_t* dyDesc, const void* dy,
                                    const cudnnConvolutionDescriptor_t* convDesc, const cudnnConvolutionBwdFilterAlgo_t* algo,
                                    void* workspace, size_t workspaceSizeInBytes,
                                    const cudnnFilterDescriptor_t* dwDesc, void* dw);
  static bool convGenBiasGradient(const cudnnTensorDescriptor_t* dyDesc, const void* dy,
                                  const cudnnTensorDescriptor_t* dbDesc, void* db);

  static bool poolFeedForward(const cudnnPoolingDescriptor_t* poolDesc, const cudnnTensorDescriptor_t* xDesc, const void* x, const cudnnTensorDescriptor_t* yDesc, void* y);
  static bool poolBackPropagate(const cudnnPoolingDescriptor_t* poolDesc, const cudnnTensorDescriptor_t* yDesc, const void* y, const cudnnTensorDescriptor_t* dyDesc, const void* dy,
                                const cudnnTensorDescriptor_t* xDesc, const void* x, const cudnnTensorDescriptor_t* dxDesc, void* dx);

  static bool activFeedForward(const cudnnActivationDescriptor_t* activDesc, const cudnnTensorDescriptor_t* srcDesc, const void* src, const cudnnTensorDescriptor_t* destDesc, void* dest);
  static bool activBackPropagate(const cudnnActivationDescriptor_t* activDesc, const cudnnTensorDescriptor_t* srcDesc, const void* src,
                                 const cudnnTensorDescriptor_t* srcDiffDesc, const void* srcDiff, const cudnnTensorDescriptor_t* destDesc,
                                 const void* dest, const cudnnTensorDescriptor_t* destDiffDesc, void* destDiff);
  static bool lrnFeedForward(const cudnnLRNDescriptor_t* normDesc, const cudnnTensorDescriptor_t* xDesc, const void* x,
                             const cudnnTensorDescriptor_t* yDesc, void* y);
  static bool lrnBackPropagate(const cudnnLRNDescriptor_t* normDesc, const cudnnTensorDescriptor_t* yDesc, const void* y,
                               const cudnnTensorDescriptor_t* dyDesc, const void* dy, const cudnnTensorDescriptor_t* xDesc, const void* x,
                               const cudnnTensorDescriptor_t* dxDesc, void* dx);

private:
  static cudnnHandle_t getCudnnHandle();

private:
  static boost::thread_specific_ptr<cudnnHandle_t> cudnnHandle;
};

const int CUDA_NUM_THREADS = 512;

// Specify workspace limit for kernels directly until we have a
// planning strategy and a rewrite of GPU memory management
const size_t CUDA_MEM_LIM = 8*1024*1024;

const float CUDA_ONE = 1.0;
const float CUDA_ZERO = 0.0;

#endif
