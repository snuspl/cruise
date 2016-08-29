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
package edu.snu.cay.dolphin.async.dnn.layers.cuda;

import org.bytedeco.javacpp.FloatPointer;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.Pointer;
import org.bytedeco.javacpp.annotation.Cast;
import org.bytedeco.javacpp.annotation.Platform;

/**
 * Java-Cudnn JNI wrapper.
 */
@Platform(include = "JavaCudnn.h", link = {"boost_thread", "cudart", "cublas", "cudnn", "javacuda"})
class JavaCudnn extends Pointer {
  static {
    Loader.load();
  }

  JavaCudnn() {
    allocate();
  }
  private native void allocate();

  @Cast(value = "cudnnTensorDescriptor_t*") static native Pointer createTensorDesc(
      final int n, final int c, final int h, final int w,
      final int nStride, final int cStride, final int hStride, final int wStride);
  @Cast(value = "cudnnTensorDescriptor_t*") static native Pointer createTensorDesc(
      final int n, final int c, final int h, final int w);
  @Cast(value = "cudnnFilterDescriptor_t*") static native Pointer createFilterDesc(
      final int k, final int c, final int h, final int w);
  @Cast(value = "cudnnConvolutionDescriptor_t*") static native Pointer createConvDesc(
      final int padH, final int padW, final int strideH, final int strideW);
  @Cast(value = "cudnnPoolingDescriptor_t*") static native Pointer createPoolDesc(
      final int mode, final int h, final int w, final int padH, final int padW, final int strideH, final int strideW);

  @Cast(value = "void*") static native Pointer getWorkspace(@Cast(value = "size_t") final long workspaceSizeInBytes);

  @Cast(value = "cudnnConvolutionFwdAlgo_t*") static native Pointer getConvForwardAlgo(
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer xDesc,
      @Cast(value = "cudnnFilterDescriptor_t*") final Pointer wDesc,
      @Cast(value = "cudnnConvolutionDescriptor_t*") final Pointer convDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer yDesc);
  @Cast(value = "cudnnConvolutionBwdDataAlgo_t*") static native Pointer getConvBackwardDataAlgo(
      @Cast(value = "cudnnFilterDescriptor_t*") final Pointer wDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dyDesc,
      @Cast(value = "cudnnConvolutionDescriptor_t*") final Pointer convDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dxDesc);
  @Cast(value = "cudnnConvolutionBwdFilterAlgo_t*") static native Pointer getConvBackwardFilterAlgo(
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer xDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dyDesc,
      @Cast(value = "cudnnConvolutionDescriptor_t*") final Pointer convDesc,
      @Cast(value = "cudnnFilterDescriptor_t*") final Pointer dwDesc);

  @Cast(value = "size_t") static native long getConvForwardWorkspaceSizeInBytes(
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer xDesc,
      @Cast(value = "cudnnFilterDescriptor_t*") final Pointer wDesc,
      @Cast(value = "cudnnConvolutionDescriptor_t*") final Pointer convDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer yDesc,
      @Cast(value = "cudnnConvolutionFwdAlgo_t*") final Pointer algo);
  @Cast(value = "size_t") static native long getConvBackwardDataWorkspaceSizeInBytes(
      @Cast(value = "cudnnFilterDescriptor_t*") final Pointer wDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dyDesc,
      @Cast(value = "cudnnConvolutionDescriptor_t*") final Pointer convDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dxDesc,
      @Cast(value = "cudnnConvolutionBwdDataAlgo_t*") final Pointer algo);
  @Cast(value = "size_t") static native long getConvBackwardFilterWorkspaceSizeInBytes(
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer xDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dyDesc,
      @Cast(value = "const cudnnConvolutionDescriptor_t*") final Pointer convDesc,
      @Cast(value = "cudnnFilterDescriptor_t*") final Pointer dwDesc,
      @Cast(value = "cudnnConvolutionBwdFilterAlgo_t*") final Pointer algo);

  @Cast(value = "bool") static native boolean convFeedForward(
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer xDesc, final FloatPointer x,
      @Cast(value = "cudnnFilterDescriptor_t*") final Pointer wDesc, final FloatPointer w,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer bDesc, final FloatPointer b,
      @Cast(value = "cudnnConvolutionDescriptor_t*") final Pointer convDesc,
      @Cast(value = "cudnnConvolutionFwdAlgo_t*") final Pointer algo,
      final Pointer workspace, @Cast(value = "size_t") final long workspaceSizeInBytes,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer yDesc, final FloatPointer y);
  @Cast(value = "bool") static native boolean convBackPropagate(
      @Cast(value = "cudnnFilterDescriptor_t*") final Pointer wDesc, final FloatPointer w,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dyDesc, final FloatPointer dy,
      @Cast(value = "cudnnConvolutionDescriptor_t*") final Pointer convDesc,
      @Cast(value = "cudnnConvolutionBwdDataAlgo_t*") final Pointer algo,
      final Pointer workspace, @Cast(value = "size_t") final long workspaceSizeInBytes,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dxDesc, final FloatPointer dx);
  @Cast(value = "bool") static native boolean convGenWeightGradient(
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer xDesc, final FloatPointer x,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dyDesc, final FloatPointer dy,
      @Cast(value = "cudnnConvolutionDescriptor_t*") final Pointer convDesc,
      @Cast(value = "cudnnConvolutionBwdFilterAlgo_t*") final Pointer algo,
      final Pointer workspace, @Cast(value = "size_t") final long workspaceSizeInBytes,
      @Cast(value = "cudnnFilterDescriptor_t*") final Pointer dwDesc, final FloatPointer dw);
  @Cast(value = "bool") static native boolean convGenBiasGradient(
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dyDesc, final FloatPointer dy,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dbDesc, final FloatPointer db);

  @Cast(value = "bool") static native boolean poolFeedForward(
      @Cast(value = "cudnnPoolingDescriptor_t*") final Pointer poolDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer xDesc, final FloatPointer x,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer yDesc, final FloatPointer y);
  @Cast(value = "bool") static native boolean poolBackPropagate(
      @Cast(value = "cudnnPoolingDescriptor_t*") final Pointer poolDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer yDesc, final FloatPointer y,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dyDesc, final FloatPointer dy,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer xDesc, final FloatPointer x,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dxDesc, final FloatPointer dx);
}
