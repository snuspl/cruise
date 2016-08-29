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
#include <iostream>

#include "JavaCudnn.h"

bool cudnnCheck(const cudnnStatus_t condition) {
  return (condition == CUDNN_STATUS_SUCCESS);
}

void* deviceMalloc(const size_t size) {
  void* devPtr;
  if (cudaSuccess != cudaMalloc(&devPtr, size)) {
    devPtr = NULL;
  }
  return devPtr;
}

void freeCudnnHandle(cudnnHandle_t* handle) {
  if (*handle != NULL) {
    cudnnDestroy(*handle);
  }
  delete handle;
}

boost::thread_specific_ptr<cudnnHandle_t> JavaCudnn::cudnnHandle(freeCudnnHandle);

cudnnHandle_t JavaCudnn::getCudnnHandle() {
  if (!cudnnHandle.get()) {
    // allocate new cudnn handle
    cudnnHandle_t* newHandlePtr = new cudnnHandle_t;
    if (CUDNN_STATUS_SUCCESS != cudnnCreate(newHandlePtr)) {
      throw std::runtime_error("Cannot create Cudnn handle. Cudnn won't be available.");
    }
    cudnnHandle.reset(newHandlePtr);
  }
  return *cudnnHandle.get();
}

cudnnTensorDescriptor_t* JavaCudnn::createTensorDesc(const int n, const int c, const int h, const int w,
                                                     const int nStride, const int cStride, const int hStride, const int wStride) {
  //cudnnTensorDescriptor_t* tensorDesc = static_cast<cudnnTensorDescriptor_t*>(deviceMalloc(sizeof(cudnnTensorDescriptor_t)));
  // if (tensorDesc != NULL) {
  //   if (cudnnCheck(cudnnCreateTensorDescriptor(tensorDesc))) {
  //     if (cudnnCheck(cudnnSetTensor4dDescriptorEx(*tensorDesc, CUDNN_DATA_FLOAT, n, c, h, w, nStride, cStride, hStride, wStride))) {
  //       return tensorDesc;
  //     } else {
  //       return NULL;
  //     }
  //   } else {
  //     return NULL;
  //   }
  // } else {
  //   return NULL;
  // }
  //cudnnTensorDescriptor_t tensorDesc;
  cudnnTensorDescriptor_t* tensorDesc = ((cudnnTensorDescriptor_t*) std::malloc(sizeof(cudnnTensorDescriptor_t)));
  if (!cudnnCheck(cudnnCreateTensorDescriptor(tensorDesc)) || !cudnnCheck(cudnnSetTensor4dDescriptorEx(*tensorDesc, CUDNN_DATA_FLOAT, n, c, h, w, nStride, cStride, hStride, wStride))) {
    return NULL;
  } else {
    return tensorDesc;
  }
}

cudnnTensorDescriptor_t* JavaCudnn::createTensorDesc(const int n, const int c, const int h, const int w) {
  int wStride = 1;
  int hStride = w * wStride;
  int cStride = h * hStride;
  int nStride = c * cStride;
  return createTensorDesc(n, c, h, w, nStride, cStride, hStride, wStride);
}

cudnnFilterDescriptor_t* JavaCudnn::createFilterDesc(const int k, const int c, const int h, const int w) {
  //cudnnFilterDescriptor_t* filterDesc = static_cast<cudnnFilterDescriptor_t*>(deviceMalloc(sizeof(cudnnFilterDescriptor_t)));
  // if (filterDesc != NULL) {
  //   if (cudnnCheck(cudnnCreateFilterDescriptor(filterDesc))) {
  //     if (cudnnCheck(cudnnSetFilter4dDescriptor(*filterDesc, CUDNN_DATA_FLOAT, CUDNN_TENSOR_NCHW, k, c, h, w))) {
  //       return filterDesc;
  //     } else {
  //       return NULL;
  //     }
  //   } else {
  //     return NULL;
  //   }
  // } else {
  //   return NULL;
  // }
  //cudnnFilterDescriptor_t filterDesc;
  cudnnFilterDescriptor_t* filterDesc = ((cudnnFilterDescriptor_t*) std::malloc(sizeof(cudnnFilterDescriptor_t)));
  if (!cudnnCheck(cudnnCreateFilterDescriptor(filterDesc)) || !cudnnCheck(cudnnSetFilter4dDescriptor(*filterDesc, CUDNN_DATA_FLOAT, CUDNN_TENSOR_NCHW, k, c, h, w))) {
    return NULL;
  } else {
    return filterDesc;
  }
}

cudnnConvolutionDescriptor_t* JavaCudnn::createConvDesc(const int padH, const int padW, const int strideH, const int strideW) {
  //cudnnConvolutionDescriptor_t* convDesc = static_cast<cudnnConvolutionDescriptor_t*>(deviceMalloc(sizeof(cudnnConvolutionDescriptor_t)));
  // if (convDesc != NULL) {
  //   if (cudnnCheck(cudnnCheck(cudnnCreateConvolutionDescriptor(convDesc))) {
  //     if (cudnnCheck(cudnnSetConvolution2dDescriptor(*convDesc, padH, padW, strideH, strideW, 1, 1, CUDNN_CROSS_CORRELATION))) {
  //       return convDesc;
  //     } else {
  //       return NULL;
  //     }
  //   } else {
  //     return NULL;
  //   }
  // } else {
  //   return NULL;
  // }
  //cudnnConvolutionDescriptor_t convDesc;
  cudnnConvolutionDescriptor_t* convDesc = ((cudnnConvolutionDescriptor_t*) std::malloc(sizeof(cudnnConvolutionDescriptor_t)));
  if (!cudnnCheck(cudnnCreateConvolutionDescriptor(convDesc)) || !cudnnCheck(cudnnSetConvolution2dDescriptor(*convDesc, padH, padW, strideH, strideW, 1, 1, CUDNN_CROSS_CORRELATION))) {
    return NULL;
  } else {
    return convDesc;
  }
}

void* JavaCudnn::getWorkspace(size_t workspaceSizeInBytes) {
  return std::malloc(workspaceSizeInBytes);
}

cudnnConvolutionFwdAlgo_t* JavaCudnn::getConvForwardAlgo(const cudnnTensorDescriptor_t* xDesc, const cudnnFilterDescriptor_t* wDesc,
                                    const cudnnConvolutionDescriptor_t* convDesc, const cudnnTensorDescriptor_t* yDesc) {

  cudnnConvolutionFwdAlgo_t* algo = ((cudnnConvolutionFwdAlgo_t*) std::malloc (sizeof(cudnnConvolutionFwdAlgo_t)));
  *algo = (cudnnConvolutionFwdAlgo_t) 0;

  if (cudnnCheck(cudnnGetConvolutionForwardAlgorithm(getCudnnHandle(), *xDesc, *wDesc, *convDesc, *yDesc, CUDNN_CONVOLUTION_FWD_SPECIFY_WORKSPACE_LIMIT, CUDA_MEM_LIM, algo))) {
    return algo;
  } else {
    return NULL;
  }
}

size_t JavaCudnn::getConvForwardWorkspaceSizeInBytes(const cudnnTensorDescriptor_t* xDesc, const cudnnFilterDescriptor_t* wDesc,
                                                     const cudnnConvolutionDescriptor_t* convDesc, const cudnnTensorDescriptor_t* yDesc, const cudnnConvolutionFwdAlgo_t* algo) {
  size_t fwdWorkspace = 0;
  if (cudnnCheck(cudnnGetConvolutionForwardWorkspaceSize(getCudnnHandle(), *xDesc, *wDesc, *convDesc, *yDesc, *algo, &fwdWorkspace))) {
    return fwdWorkspace;
  } else {
    return 0;
  }
}

cudnnConvolutionBwdDataAlgo_t* JavaCudnn::getConvBackwardDataAlgo(const cudnnFilterDescriptor_t* wDesc, const cudnnTensorDescriptor_t* dyDesc, const cudnnConvolutionDescriptor_t* convDesc, const cudnnTensorDescriptor_t* dxDesc) {
  cudnnConvolutionBwdDataAlgo_t* algo = ((cudnnConvolutionBwdDataAlgo_t*) std::malloc (sizeof(cudnnConvolutionBwdDataAlgo_t)));
  *algo = (cudnnConvolutionBwdDataAlgo_t) 0;
  if (cudnnCheck(cudnnGetConvolutionBackwardDataAlgorithm(getCudnnHandle(), *wDesc, *dyDesc, *convDesc, *dxDesc, CUDNN_CONVOLUTION_BWD_DATA_SPECIFY_WORKSPACE_LIMIT, CUDA_MEM_LIM, algo))) {
   return algo;
 } else {
  return NULL;
 }
}

size_t JavaCudnn::getConvBackwardDataWorkspaceSizeInBytes(const cudnnFilterDescriptor_t* wDesc, const cudnnTensorDescriptor_t* dyDesc,
                                                          const cudnnConvolutionDescriptor_t* convDesc, const cudnnTensorDescriptor_t* dxDesc,
                                                          const cudnnConvolutionBwdDataAlgo_t* algo) {
  size_t bwdDataWorkspace = 0;
  if (cudnnCheck(cudnnGetConvolutionBackwardDataWorkspaceSize(getCudnnHandle(), *wDesc, *dyDesc, *convDesc, *dxDesc, *algo, &bwdDataWorkspace))) {
    return bwdDataWorkspace;
  } else {
    return 0;
  }
}

cudnnConvolutionBwdFilterAlgo_t* JavaCudnn::getConvBackwardFilterAlgo(const cudnnTensorDescriptor_t* xDesc, const cudnnTensorDescriptor_t* dyDesc,
                                                                      const cudnnConvolutionDescriptor_t* convDesc, const cudnnFilterDescriptor_t* dwDesc) {
  cudnnConvolutionBwdFilterAlgo_t* algo = ((cudnnConvolutionBwdFilterAlgo_t*) std::malloc (sizeof(cudnnConvolutionBwdFilterAlgo_t)));
  *algo = (cudnnConvolutionBwdFilterAlgo_t) 0;

  if (cudnnCheck(cudnnGetConvolutionBackwardFilterAlgorithm(getCudnnHandle(), *xDesc, *dyDesc, *convDesc, *dwDesc, CUDNN_CONVOLUTION_BWD_FILTER_SPECIFY_WORKSPACE_LIMIT, CUDA_MEM_LIM, algo))) {
    return algo;
  }
  else {
   return NULL;
  }
}

size_t JavaCudnn::getConvBackwardFilterWorkspaceSizeInBytes(const cudnnTensorDescriptor_t* xDesc, const cudnnTensorDescriptor_t* dyDesc,
                                                            const cudnnConvolutionDescriptor_t* convDesc, const cudnnFilterDescriptor_t* dwDesc, const cudnnConvolutionBwdFilterAlgo_t* algo) {
  size_t bwdFilterWorkspace = 0;
  if (cudnnCheck(cudnnGetConvolutionBackwardFilterWorkspaceSize(getCudnnHandle(), *xDesc, *dyDesc, *convDesc, *dwDesc, *algo, &bwdFilterWorkspace))) {
    return bwdFilterWorkspace;
  } else {
    return 0;
  }
}

bool JavaCudnn::convFeedForward(const cudnnTensorDescriptor_t* xDesc, const void* x,
                                const cudnnFilterDescriptor_t* wDesc, const void* w,
                                const cudnnTensorDescriptor_t* bDesc, const void* b,
                                const cudnnConvolutionDescriptor_t* convDesc, const cudnnConvolutionFwdAlgo_t* algo,
                                void* workspace, size_t workspaceSizeInBytes,
                                const cudnnTensorDescriptor_t* yDesc, void* y) {
  if (cudnnCheck(cudnnConvolutionForward(getCudnnHandle(), &CUDA_ONE ,*xDesc, x, *wDesc, w, *convDesc, *algo, workspace, workspaceSizeInBytes, &CUDA_ZERO, *yDesc, y))) {
    return cudnnCheck(cudnnAddTensor(getCudnnHandle(), &CUDA_ONE, *bDesc, b, &CUDA_ONE, *yDesc, y));
  } else {
    return false;
  }
}

bool JavaCudnn::convBackPropagate(const cudnnFilterDescriptor_t* wDesc, const void* w, const cudnnTensorDescriptor_t* dyDesc, const void* dy,
                                  const cudnnConvolutionDescriptor_t* convDesc, const cudnnConvolutionBwdDataAlgo_t* algo, void* workspace,
                                  size_t workspaceSizeInBytes, const cudnnTensorDescriptor_t* dxDesc, void* dx) {
  return cudnnCheck(cudnnConvolutionBackwardData(getCudnnHandle(), &CUDA_ONE, *wDesc, w, *dyDesc, dy, *convDesc, *algo, workspace, workspaceSizeInBytes, &CUDA_ZERO, *dxDesc, dx));
}

bool JavaCudnn::convGenWeightGradient(const cudnnTensorDescriptor_t* xDesc, const void* x, const cudnnTensorDescriptor_t* dyDesc, const void* dy,
                                      const cudnnConvolutionDescriptor_t* convDesc, const cudnnConvolutionBwdFilterAlgo_t* algo, void* workspace,
                                      size_t workspaceSizeInBytes, const cudnnFilterDescriptor_t* dwDesc, void* dw) {
  return cudnnCheck(cudnnConvolutionBackwardFilter(getCudnnHandle(), &CUDA_ONE, *xDesc, x, *dyDesc, dy, *convDesc, *algo, workspace, workspaceSizeInBytes, &CUDA_ONE, *dwDesc, dw));
}

bool JavaCudnn::convGenBiasGradient(const cudnnTensorDescriptor_t* dyDesc, const void* dy, const cudnnTensorDescriptor_t* dbDesc, void* db) {
  return cudnnCheck(cudnnConvolutionBackwardBias(getCudnnHandle(), &CUDA_ONE, *dyDesc, dy, &CUDA_ONE, *dbDesc, db));
}

cudnnPoolingDescriptor_t* JavaCudnn::createPoolDesc(const int mode, const int h, const int w, const int padH, const int padW, const int strideH, const int strideW) {
  cudnnPoolingDescriptor_t* poolDesc = ((cudnnPoolingDescriptor_t*) std::malloc (sizeof(cudnnPoolingDescriptor_t)));
  if(mode == 0) {
    if (!cudnnCheck(cudnnCreatePoolingDescriptor(poolDesc)) || !cudnnCheck(cudnnSetPooling2dDescriptor(*poolDesc, CUDNN_POOLING_MAX, CUDNN_PROPAGATE_NAN, h, w, padH, padW, strideH, strideW))) {
      return NULL;
    } else {
      return poolDesc;
    }
  } else {
    if (!cudnnCheck(cudnnCreatePoolingDescriptor(poolDesc)) || !cudnnCheck(cudnnSetPooling2dDescriptor(*poolDesc, CUDNN_POOLING_AVERAGE_COUNT_INCLUDE_PADDING, CUDNN_PROPAGATE_NAN, h, w, padH, padW, strideH, strideW))) {
      return NULL;
    } else {
      return poolDesc;
    }
  }
}

bool JavaCudnn::poolFeedForward(const cudnnPoolingDescriptor_t* poolDesc, const cudnnTensorDescriptor_t* xDesc, const void* x, const cudnnTensorDescriptor_t* yDesc, void* y) {
  return cudnnCheck(cudnnPoolingForward(getCudnnHandle(), *poolDesc, &CUDA_ONE, *xDesc, x, &CUDA_ZERO, *yDesc, y));
}

bool JavaCudnn::poolBackPropagate(const cudnnPoolingDescriptor_t* poolDesc, const cudnnTensorDescriptor_t* yDesc, const void* y, const cudnnTensorDescriptor_t* dyDesc, const void* dy,
                                  const cudnnTensorDescriptor_t* xDesc, const void* x, const cudnnTensorDescriptor_t* dxDesc, void* dx) {
  return cudnnCheck(cudnnPoolingBackward(getCudnnHandle(), *poolDesc, &CUDA_ONE, *yDesc, y, *dyDesc, dy, *xDesc, x, &CUDA_ZERO, *dxDesc, dx));
}

cudnnActivationDescriptor_t* JavaCudnn::createActivDesc(const int fun) {
  cudnnActivationDescriptor_t* activDesc = ((cudnnActivationDescriptor_t*) std::malloc (sizeof(cudnnActivationDescriptor_t)));
  cudnnActivationMode_t mode;
  if (fun == 0) {
    mode = CUDNN_ACTIVATION_SIGMOID;
  } else if (fun == 1) {
    mode = CUDNN_ACTIVATION_RELU;
  } else if (fun == 2) {
    mode = CUDNN_ACTIVATION_TANH;
  } else {
    mode = CUDNN_ACTIVATION_CLIPPED_RELU;
  }

  if(!cudnnCheck(cudnnCreateActivationDescriptor(activDesc)) || !cudnnCheck(cudnnSetActivationDescriptor(*activDesc, mode, CUDNN_PROPAGATE_NAN, 0.0))) {
    return NULL;
  } else {
    return activDesc;
  }
}

bool JavaCudnn::activFeedForward(const cudnnActivationDescriptor_t* activDesc, const cudnnTensorDescriptor_t* srcDesc, const void* src, const cudnnTensorDescriptor_t* destDesc, void* dest) {
  return cudnnCheck(cudnnActivationForward(getCudnnHandle(), *activDesc, &CUDA_ONE, *srcDesc, src, &CUDA_ZERO, *destDesc, dest));
}

bool JavaCudnn::activBackPropagate(const cudnnActivationDescriptor_t* activDesc, const cudnnTensorDescriptor_t* srcDesc, const void* src, const cudnnTensorDescriptor_t* srcDiffDesc, const void* srcDiff,
                                   const cudnnTensorDescriptor_t* destDesc, const void* dest, const cudnnTensorDescriptor_t* destDiffDesc, void* destDiff ) {
  return cudnnCheck(cudnnActivationBackward(getCudnnHandle(), *activDesc, &CUDA_ONE, *srcDesc, src, *srcDiffDesc, srcDiff, *destDesc, dest, &CUDA_ZERO, *destDiffDesc, destDiff));
}

cudnnLRNDescriptor_t* JavaCudnn::createLRNDesc(const int localSize, const float alpha, const float beta, const float k) {
  cudnnLRNDescriptor_t* normDesc = ((cudnnLRNDescriptor_t*) std::malloc (sizeof(cudnnLRNDescriptor_t)));
  if(!cudnnCheck(cudnnCreateLRNDescriptor(normDesc)) || !cudnnCheck(cudnnSetLRNDescriptor(*normDesc, localSize, alpha, beta, k))) {
    return NULL;
  } else {
    return normDesc;
  }
}

bool JavaCudnn::lrnFeedForward(const cudnnLRNDescriptor_t* normDesc, const cudnnTensorDescriptor_t* xDesc, const void* x,
                               const cudnnTensorDescriptor_t* yDesc, void* y) {
 return cudnnCheck(cudnnLRNCrossChannelForward(getCudnnHandle(), *normDesc, CUDNN_LRN_CROSS_CHANNEL_DIM1, &CUDA_ONE, *xDesc, x, &CUDA_ZERO, *yDesc, y));
}

bool JavaCudnn::lrnBackPropagate(const cudnnLRNDescriptor_t* normDesc, const cudnnTensorDescriptor_t* yDesc, const void* y,
                                 const cudnnTensorDescriptor_t* dyDesc, const void* dy, const cudnnTensorDescriptor_t* xDesc, const void* x,
                                 const cudnnTensorDescriptor_t* dxDesc, void* dx) {
 return cudnnCheck(cudnnLRNCrossChannelBackward(getCudnnHandle(), *normDesc, CUDNN_LRN_CROSS_CHANNEL_DIM1, &CUDA_ONE, *yDesc, y, *dyDesc, dy, *xDesc, x, &CUDA_ZERO, *dxDesc, dx));
}