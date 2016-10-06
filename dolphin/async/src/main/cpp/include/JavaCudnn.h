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

/*
 * Native methods for JavaCudnn.
 * Please refer to followings:
 * https://developer.nvidia.com/cudnn
 * https://developer.nvidia.com/rdp/cudnn-download (requires registration)
 */

class JavaCudnn {
public:
  // destroy pointer which is created to support cudnn.
  static bool destroyPointer(void* pointer);
  static cudnnTensorDescriptor_t* cudnnCreateTensorDesc(
      const int n, const int c, const int h, const int w,
      const int nStride, const int cStride, const int hStride, const int wStride);
  static cudnnTensorDescriptor_t* cudnnCreateTensorDesc(const int n, const int c, const int h, const int w);
  static bool cudnnDestroyTensorDesc(cudnnTensorDescriptor_t* tensorDesc);
  static cudnnFilterDescriptor_t* cudnnCreateFilterDesc(const int k, const int c, const int h, const int w);
  static bool cudnnDestroyFilterDesc(cudnnFilterDescriptor_t* filterDesc);
  static cudnnConvolutionDescriptor_t* cudnnCreateConvDesc(
      const int padH, const int padW, const int strideH, const int strideW);
  static bool cudnnDestroyConvDesc(cudnnConvolutionDescriptor_t* convDesc);
  static cudnnPoolingDescriptor_t* cudnnCreatePoolDesc(
      const char mode, const int h, const int w, const int padH, const int padW, const int strideH, const int strideW);
  static bool cudnnDestroyPoolDesc(cudnnPoolingDescriptor_t* poolDesc);
  static cudnnActivationDescriptor_t* cudnnCreateActivFuncDesc(const char func);
  static bool cudnnDestroyActivFuncDesc(cudnnActivationDescriptor_t*);
  static cudnnLRNDescriptor_t* cudnnCreateLRNDesc(
      const int localSize, const float alpha, const float beta, const float k);
  static bool cudnnDestroyLRNDesc(cudnnLRNDescriptor_t* lrnDesc);

  static cudnnConvolutionFwdAlgo_t* cudnnGetConvForwardAlgo(
      const cudnnTensorDescriptor_t* xDesc, const cudnnFilterDescriptor_t* wDesc,
      const cudnnConvolutionDescriptor_t* convDesc, const cudnnTensorDescriptor_t* yDesc);
  static cudnnConvolutionBwdDataAlgo_t* cudnnGetConvBackwardDataAlgo(
      const cudnnFilterDescriptor_t* wDesc, const cudnnTensorDescriptor_t* dyDesc,
      const cudnnConvolutionDescriptor_t* convDesc, const cudnnTensorDescriptor_t* dxDesc);
  static cudnnConvolutionBwdFilterAlgo_t* cudnnGetConvBackwardFilterAlgo(
      const cudnnTensorDescriptor_t* xDesc, const cudnnTensorDescriptor_t* dyDesc,
      const cudnnConvolutionDescriptor_t* convDesc, const cudnnFilterDescriptor_t* dwDesc);

  static size_t getConvForwardWorkspaceSizeInBytes(
      const cudnnTensorDescriptor_t* xDesc, const cudnnFilterDescriptor_t* wDesc,
      const cudnnConvolutionDescriptor_t* convDesc, const cudnnTensorDescriptor_t* yDesc,
      const cudnnConvolutionFwdAlgo_t* algo);
  static size_t getConvBackwardDataWorkspaceSizeInBytes(
      const cudnnFilterDescriptor_t* wDesc, const cudnnTensorDescriptor_t* dyDesc,
      const cudnnConvolutionDescriptor_t* convDesc, const cudnnTensorDescriptor_t* dxDesc,
      const cudnnConvolutionBwdDataAlgo_t* algo);
  static size_t getConvBackwardFilterWorkspaceSizeInBytes(
      const cudnnTensorDescriptor_t* xDesc, const cudnnTensorDescriptor_t* dyDesc,
      const cudnnConvolutionDescriptor_t* convDesc, const cudnnFilterDescriptor_t* dwDesc,
      const cudnnConvolutionBwdFilterAlgo_t* algo);

  static bool convFeedForward(
      const cudnnTensorDescriptor_t* xDesc, const void* x, const cudnnFilterDescriptor_t* wDesc, const void* w,
      const cudnnTensorDescriptor_t* bDesc, const void* b, const cudnnConvolutionDescriptor_t* convDesc,
      const cudnnConvolutionFwdAlgo_t* algo, void* workspace, size_t workspaceSizeInBytes,
      const cudnnTensorDescriptor_t* yDesc, void* y);
  static bool convBackPropagate(
      const cudnnFilterDescriptor_t* wDesc, const void* w, const cudnnTensorDescriptor_t* dyDesc, const void* dy,
      const cudnnConvolutionDescriptor_t* convDesc,
      const cudnnConvolutionBwdDataAlgo_t* algo, void* workspace, size_t workspaceSizeInBytes,
      const cudnnTensorDescriptor_t* dxDesc, void* dx);
  static bool convGenWeightGradient(
      const cudnnTensorDescriptor_t* xDesc, const void* x, const cudnnTensorDescriptor_t* dyDesc, const void* dy,
      const cudnnConvolutionDescriptor_t* convDesc,
      const cudnnConvolutionBwdFilterAlgo_t* algo, void* workspace, size_t workspaceSizeInBytes,
      const cudnnFilterDescriptor_t* dwDesc, void* dw);
  static bool convGenBiasGradient(
      const cudnnTensorDescriptor_t* dyDesc, const void* dy, const cudnnTensorDescriptor_t* dbDesc, void* db);
  static bool poolFeedForward(
      const cudnnPoolingDescriptor_t* poolDesc,
      const cudnnTensorDescriptor_t* xDesc, const void* x,
      const cudnnTensorDescriptor_t* yDesc, void* y);
  static bool poolBackPropagate(
      const cudnnPoolingDescriptor_t* poolDesc,
      const cudnnTensorDescriptor_t* yDesc, const void* y,
      const cudnnTensorDescriptor_t* dyDesc, const void* dy,
      const cudnnTensorDescriptor_t* xDesc, const void* x,
      const cudnnTensorDescriptor_t* dxDesc, void* dx);

  static bool activFeedForward(
      const cudnnActivationDescriptor_t* activDesc,
      const cudnnTensorDescriptor_t* srcDesc, const void* src,
      const cudnnTensorDescriptor_t* destDesc, void* dest);
  static bool activBackPropagate(
      const cudnnActivationDescriptor_t* activDesc,
      const cudnnTensorDescriptor_t* srcDesc, const void* src,
      const cudnnTensorDescriptor_t* srcDiffDesc, const void* srcDiff,
      const cudnnTensorDescriptor_t* destDesc, const void* dest,
      const cudnnTensorDescriptor_t* destDiffDesc, void* destDiff);
  static bool activWithLossFeedForward (
      const cudnnTensorDescriptor_t* xDesc, const void* x, const cudnnTensorDescriptor_t* yDesc, void* y);
  static bool activWithLossBackPropagate (
      const cudnnTensorDescriptor_t* yDesc, const void* y,
      const cudnnTensorDescriptor_t* dyDesc, const void* dy,
      const cudnnTensorDescriptor_t* dxDesc, void* dx);
  static bool lrnFeedForward(
      const cudnnLRNDescriptor_t* normDesc,
      const cudnnTensorDescriptor_t* xDesc, const void* x,
      const cudnnTensorDescriptor_t* yDesc, void* y);
  static bool lrnBackPropagate(
      const cudnnLRNDescriptor_t* normDesc,
      const cudnnTensorDescriptor_t* yDesc, const void* y,
      const cudnnTensorDescriptor_t* dyDesc, const void* dy,
      const cudnnTensorDescriptor_t* xDesc, const void* x,
      const cudnnTensorDescriptor_t* dxDesc, void* dx);

private:
  static cudnnHandle_t getCudnnHandle();

private:
  static boost::thread_specific_ptr<cudnnHandle_t> cudnnHandle;
};

// Specify workspace limit for kernels directly until we have a planning strategy and a rewrite of GPU memory management
const size_t CUDA_MEM_LIM = 8*1024*1024;

const float CUDA_ONE = 1.0;
const float CUDA_ZERO = 0.0;

#endif
