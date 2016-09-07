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

  private static void checkNullPointer(final Pointer ptr) {
    if (ptr == null) {
      throw new RuntimeException("Null was passed for pointer");
    }
  }

  @Cast(value = "cudnnTensorDescriptor_t*") static native Pointer cudnnCreateTensorDesc(
      final int n, final int c, final int h, final int w,
      final int nStride, final int cStride, final int hStride, final int wStride);
  @Cast(value = "cudnnTensorDescriptor_t*") static native Pointer cudnnCreateTensorDesc(
      final int n, final int c, final int h, final int w);
  static Pointer createTensorDesc(final int n, final int c, final int h, final int w) {
    final Pointer pointer = cudnnCreateTensorDesc(n, c, h, w);
    checkNullPointer(pointer);
    return pointer;
  }
  @Cast(value = "cudnnFilterDescriptor_t*") static native Pointer cudnnCreateFilterDesc(
      final int k, final int c, final int h, final int w);
  static Pointer createFilterDesc(final int k, final int c, final int h, final int w) {
    final Pointer pointer = cudnnCreateFilterDesc(k, c, h, w);
    checkNullPointer(pointer);
    return pointer;
  }
  @Cast(value = "cudnnConvolutionDescriptor_t*") static native Pointer cudnnCreateConvDesc(
      final int padH, final int padW, final int strideH, final int strideW);
  static Pointer createConvDesc(final int padH, final int padW, final int strideH, final int strideW) {
    final Pointer pointer = cudnnCreateConvDesc(padH, padW, strideH, strideW);
    checkNullPointer(pointer);
    return pointer;
  }
  @Cast(value = "cudnnPoolingDescriptor_t*") static native Pointer cudnnCreatePoolDesc(
      final char mode, final int h, final int w, final int padH, final int padW, final int strideH, final int strideW);
  static Pointer createPoolDesc(final char mode, final int h, final int w, final int padH, final int padW,
                                final int strideH, final int strideW) {
    final Pointer pointer = cudnnCreatePoolDesc(mode, h, w, padH, padW, strideH, strideW);
    checkNullPointer(pointer);
    return pointer;
  }
  @Cast(value = "cudnnActivationDescriptor_t*") static native Pointer cudnnCreateActivDesc(final char func);
  static Pointer createActivDesc(final char func) {
    final Pointer pointer = cudnnCreateActivDesc(func);
    checkNullPointer(pointer);
    return pointer;
  }
  @Cast(value = "cudnnLRNDescriptor_t*") static native Pointer cudnnCreateLRNDesc(
      final int localSize, final float alpha, final float beta, final float k);
  static Pointer createLRNDesc(final int localSize, final float alpha, final float beta, final float k) {
    final Pointer pointer = cudnnCreateLRNDesc(localSize, alpha, beta, k);
    checkNullPointer(pointer);
    return pointer;
  }

  @Cast(value = "cudnnConvolutionFwdAlgo_t*") static native Pointer cudnnGetConvForwardAlgo(
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer xDesc,
      @Cast(value = "cudnnFilterDescriptor_t*") final Pointer wDesc,
      @Cast(value = "cudnnConvolutionDescriptor_t*") final Pointer convDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer yDesc);
  static Pointer getConvForwardAlgo(
      final Pointer xDesc, final Pointer wDesc, final Pointer convDesc, final Pointer yDesc) {
    final Pointer pointer = cudnnGetConvForwardAlgo(xDesc, wDesc, convDesc, yDesc);
    checkNullPointer(pointer);
    return pointer;
  }
  @Cast(value = "cudnnConvolutionBwdDataAlgo_t*") static native Pointer cudnnGetConvBackwardDataAlgo(
      @Cast(value = "cudnnFilterDescriptor_t*") final Pointer wDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dyDesc,
      @Cast(value = "cudnnConvolutionDescriptor_t*") final Pointer convDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dxDesc);
  static Pointer getConvBackwardDataAlgo(
      final Pointer wDesc, final Pointer dyDesc, final Pointer convDesc, final Pointer dxDesc) {
    final Pointer pointer = cudnnGetConvBackwardDataAlgo(wDesc, dyDesc, convDesc, dxDesc);
    checkNullPointer(pointer);
    return pointer;
  }
  @Cast(value = "cudnnConvolutionBwdFilterAlgo_t*") static native Pointer cudnnGetConvBackwardFilterAlgo(
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer xDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dyDesc,
      @Cast(value = "cudnnConvolutionDescriptor_t*") final Pointer convDesc,
      @Cast(value = "cudnnFilterDescriptor_t*") final Pointer dwDesc);
  static Pointer getConvBackwardFilterAlgo(
      final Pointer xDesc, final Pointer dyDesc, final Pointer convDesc, final Pointer dwDesc) {
    final Pointer pointer = cudnnGetConvBackwardFilterAlgo(xDesc, dyDesc, convDesc, dwDesc);
    checkNullPointer(pointer);
    return pointer;
  }

  @Cast(value = "void*") static native Pointer cudnnGetWorkspace(
      @Cast(value = "size_t") final long workspaceSizeInBytes);
  static Pointer getWorkspace(final long workspace) {
    if (workspace == 0) {
      return null;
    } else {
      final Pointer pointer = cudnnGetWorkspace(workspace);
      checkNullPointer(pointer);
      return pointer;
    }
  }
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
  @Cast(value = "bool") static native boolean activFeedForward(
      @Cast(value = "cudnnActivationDescriptor_t*") final Pointer activDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer srcDesc, final FloatPointer src,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer destDesc, final FloatPointer dest);
  @Cast(value = "bool") static native boolean activBackPropagate(
      @Cast(value = "cudnnActivationDescriptor_t*") final Pointer activDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer srcDesc, final FloatPointer src,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer srcDiffDesc, final FloatPointer srcDiff,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer destDesc, final FloatPointer dest,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer destDiffDesc, final FloatPointer destDiff);
  @Cast(value = "bool") static native boolean lrnFeedForward(
      @Cast(value = "cudnnLRNDescriptor_t*") final Pointer normDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer xDesc, final FloatPointer x,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer yDesc, final FloatPointer y);
  @Cast(value = "bool") static native boolean lrnBackPropagate(
      @Cast(value = "cudnnLRNDescriptor_t*") final Pointer normDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer yDesc, final FloatPointer y,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dyDesc, final FloatPointer dy,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer xDesc, final FloatPointer x,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dxDesc, final FloatPointer dx);
  @Cast(value = "bool") static native boolean activWithLossFeedForward(
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer xDesc, final FloatPointer x,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer yDesc, final FloatPointer y);
  @Cast(value = "bool") static native boolean activWithLossBackPropagate(
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer yDesc, final FloatPointer y,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dyDesc, final FloatPointer dy,
      @Cast(value = "cudnnTensorDescriptor_t*") final Pointer dxDesc, final FloatPointer dx);
}
