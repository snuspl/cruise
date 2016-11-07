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
    if (ptr.isNull()) {
      throw new RuntimeException("Null was passed for pointer");
    }
  }

  static native boolean destroyPointer(@Cast(value = "void*") Pointer pointer);
  @Cast(value = "cudnnTensorDescriptor_t*") static native Pointer cudnnCreateTensorDesc(
      int n, int c, int h, int w,
      int nStride, int cStride, int hStride, int wStride);
  @Cast(value = "cudnnTensorDescriptor_t*") static native Pointer cudnnCreateTensorDesc(
      int n, int c, int h, int w);
  static Pointer createTensorDesc(final int n, final int c, final int h, final int w) {
    final Pointer pointer = new Pointer(cudnnCreateTensorDesc(n, c, h, w));
    checkNullPointer(pointer);
    return pointer;
  }
  static native boolean cudnnDestroyTensorDesc(@Cast(value = "cudnnTensorDescriptor_t*") Pointer pointer);
  static void destroyTensorDesc(final Pointer pointer) {
    if (!pointer.isNull() && !cudnnDestroyTensorDesc(pointer)) {
      throw new RuntimeException("Destroying tensor descriptor is failed");
    }
  }
  @Cast(value = "cudnnFilterDescriptor_t*") static native Pointer cudnnCreateFilterDesc(
      int k, int c, int h, int w);
  static Pointer createFilterDesc(final int k, final int c, final int h, final int w) {
    final Pointer pointer = new Pointer(cudnnCreateFilterDesc(k, c, h, w));
    checkNullPointer(pointer);
    return pointer;
  }
  static native boolean cudnnDestroyFilterDesc(@Cast(value = "cudnnFilterDescriptor_t*") Pointer pointer);
  static void destroyFilterDesc(final Pointer pointer) {
    if (!cudnnDestroyFilterDesc(pointer)) {
      throw new RuntimeException("Destroying filter descriptor is failed");
    }
  }
  @Cast(value = "cudnnConvolutionDescriptor_t*") static native Pointer cudnnCreateConvDesc(
      int padH, int padW, int strideH, int strideW);
  static Pointer createConvDesc(final int padH, final int padW, final int strideH, final int strideW) {
    final Pointer pointer = new Pointer(cudnnCreateConvDesc(padH, padW, strideH, strideW));
    checkNullPointer(pointer);
    return pointer;
  }
  static native boolean cudnnDestroyConvDesc(@Cast(value = "cudnnConvolutionDescriptor_t*") Pointer pointer);
  static void destroyConvDesc(final Pointer pointer) {
    if (!cudnnDestroyConvDesc(pointer)) {
      throw new RuntimeException("Destroying convolution descriptor is failed");
    }
  }
  @Cast(value = "cudnnPoolingDescriptor_t*") static native Pointer cudnnCreatePoolDesc(
      char mode, int h, int w, int padH, int padW, int strideH, int strideW);
  static Pointer createPoolDesc(final char mode, final int h, final int w, final int padH, final int padW,
                                final int strideH, final int strideW) {
    final Pointer pointer = new Pointer(cudnnCreatePoolDesc(mode, h, w, padH, padW, strideH, strideW));
    checkNullPointer(pointer);
    return pointer;
  }
  static native boolean cudnnDestroyPoolDesc(@Cast(value = "cudnnPoolingDescriptor_t*") Pointer pointer);
  static void destroyPoolDesc(final Pointer pointer) {
    if (!cudnnDestroyPoolDesc(pointer)) {
      throw new RuntimeException("Destroying pooling descriptor is failed");
    }
  }
  @Cast(value = "cudnnActivationDescriptor_t*") static native Pointer cudnnCreateActivFuncDesc(char func);
  static Pointer createActivFuncDesc(final char func) {
    final Pointer pointer = new Pointer(cudnnCreateActivFuncDesc(func));
    checkNullPointer(pointer);
    return pointer;
  }
  static native boolean cudnnDestroyActivFuncDesc(@Cast(value = "cudnnActivationDescriptor_t*") Pointer pointer);
  static void destroyActivFuncDesc(final Pointer pointer) {
    if (!cudnnDestroyActivFuncDesc(pointer)) {
      throw new RuntimeException("Destroying activation function descriptor is failed");
    }
  }
  @Cast(value = "cudnnLRNDescriptor_t*") static native Pointer cudnnCreateLRNDesc(
      int localSize, float alpha, float beta, float k);
  static Pointer createLRNDesc(final int localSize, final float alpha, final float beta, final float k) {
    final Pointer pointer = new Pointer(cudnnCreateLRNDesc(localSize, alpha, beta, k));
    checkNullPointer(pointer);
    return pointer;
  }
  static native boolean cudnnDestroyLRNDesc(@Cast(value = "cudnnLRNDescriptor_t*") Pointer pointer);
  static void destroyLRNDesc(final Pointer pointer) {
    if (!cudnnDestroyLRNDesc(pointer)) {
      throw new RuntimeException("Destroying LRN descriptor is failed");
    }
  }

  @Cast(value = "cudnnConvolutionFwdAlgo_t*") static native Pointer cudnnGetConvForwardAlgo(
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer xDesc,
      @Cast(value = "cudnnFilterDescriptor_t*") Pointer wDesc,
      @Cast(value = "cudnnConvolutionDescriptor_t*") Pointer convDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer yDesc);
  static Pointer getConvForwardAlgo(
      final Pointer xDesc, final Pointer wDesc, final Pointer convDesc, final Pointer yDesc) {
    final Pointer pointer = new Pointer(cudnnGetConvForwardAlgo(xDesc, wDesc, convDesc, yDesc));
    checkNullPointer(pointer);
    return pointer;
  }
  @Cast(value = "cudnnConvolutionBwdDataAlgo_t*") static native Pointer cudnnGetConvBackwardDataAlgo(
      @Cast(value = "cudnnFilterDescriptor_t*") Pointer wDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dyDesc,
      @Cast(value = "cudnnConvolutionDescriptor_t*") Pointer convDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dxDesc);
  static Pointer getConvBackwardDataAlgo(
      final Pointer wDesc, final Pointer dyDesc, final Pointer convDesc, final Pointer dxDesc) {
    final Pointer pointer = new Pointer(cudnnGetConvBackwardDataAlgo(wDesc, dyDesc, convDesc, dxDesc));
    checkNullPointer(pointer);
    return pointer;
  }
  @Cast(value = "cudnnConvolutionBwdFilterAlgo_t*") static native Pointer cudnnGetConvBackwardFilterAlgo(
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer xDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dyDesc,
      @Cast(value = "cudnnConvolutionDescriptor_t*") Pointer convDesc,
      @Cast(value = "cudnnFilterDescriptor_t*") Pointer dwDesc);
  static Pointer getConvBackwardFilterAlgo(
      final Pointer xDesc, final Pointer dyDesc, final Pointer convDesc, final Pointer dwDesc) {
    final Pointer pointer = new Pointer(cudnnGetConvBackwardFilterAlgo(xDesc, dyDesc, convDesc, dwDesc));
    checkNullPointer(pointer);
    return pointer;
  }
  static void destroyAlgo(final Pointer pointer) {
    if (!pointer.isNull() && !destroyPointer(pointer)) {
      throw new RuntimeException("Destroying algorithm is failed");
    }
  }

  @Cast(value = "size_t") static native long getConvForwardWorkspaceSizeInBytes(
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer xDesc,
      @Cast(value = "cudnnFilterDescriptor_t*") Pointer wDesc,
      @Cast(value = "cudnnConvolutionDescriptor_t*") Pointer convDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer yDesc,
      @Cast(value = "cudnnConvolutionFwdAlgo_t*") Pointer algo);
  @Cast(value = "size_t") static native long getConvBackwardDataWorkspaceSizeInBytes(
      @Cast(value = "cudnnFilterDescriptor_t*") Pointer wDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dyDesc,
      @Cast(value = "cudnnConvolutionDescriptor_t*") Pointer convDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dxDesc,
      @Cast(value = "cudnnConvolutionBwdDataAlgo_t*") Pointer algo);
  @Cast(value = "size_t") static native long getConvBackwardFilterWorkspaceSizeInBytes(
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer xDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dyDesc,
      @Cast(value = "const cudnnConvolutionDescriptor_t*") Pointer convDesc,
      @Cast(value = "cudnnFilterDescriptor_t*") Pointer dwDesc,
      @Cast(value = "cudnnConvolutionBwdFilterAlgo_t*") Pointer algo);

  @Cast(value = "bool") static native boolean convFeedForward(
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer xDesc, FloatPointer x,
      @Cast(value = "cudnnFilterDescriptor_t*") Pointer wDesc, FloatPointer w,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer bDesc, FloatPointer b,
      @Cast(value = "cudnnConvolutionDescriptor_t*") Pointer convDesc,
      @Cast(value = "cudnnConvolutionFwdAlgo_t*") Pointer algo,
      Pointer workspace, @Cast(value = "size_t") long workspaceSizeInBytes,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer yDesc, FloatPointer y);
  @Cast(value = "bool") static native boolean convBackPropagate(
      @Cast(value = "cudnnFilterDescriptor_t*") Pointer wDesc, FloatPointer w,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dyDesc, FloatPointer dy,
      @Cast(value = "cudnnConvolutionDescriptor_t*") Pointer convDesc,
      @Cast(value = "cudnnConvolutionBwdDataAlgo_t*") Pointer algo,
      Pointer workspace, @Cast(value = "size_t") long workspaceSizeInBytes,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dxDesc, FloatPointer dx);
  @Cast(value = "bool") static native boolean convGenWeightGradient(
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer xDesc, FloatPointer x,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dyDesc, FloatPointer dy,
      @Cast(value = "cudnnConvolutionDescriptor_t*") Pointer convDesc,
      @Cast(value = "cudnnConvolutionBwdFilterAlgo_t*") Pointer algo,
      Pointer workspace, @Cast(value = "size_t") long workspaceSizeInBytes,
      @Cast(value = "cudnnFilterDescriptor_t*") Pointer dwDesc, FloatPointer dw);
  @Cast(value = "bool") static native boolean convGenBiasGradient(
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dyDesc, FloatPointer dy,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dbDesc, FloatPointer db);
  @Cast(value = "bool") static native boolean poolFeedForward(
      @Cast(value = "cudnnPoolingDescriptor_t*") Pointer poolDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer xDesc, FloatPointer x,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer yDesc, FloatPointer y);
  @Cast(value = "bool") static native boolean poolBackPropagate(
      @Cast(value = "cudnnPoolingDescriptor_t*") Pointer poolDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer yDesc, FloatPointer y,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dyDesc, FloatPointer dy,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer xDesc, FloatPointer x,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dxDesc, FloatPointer dx);
  @Cast(value = "bool") static native boolean activFeedForward(
      @Cast(value = "cudnnActivationDescriptor_t*") Pointer activDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer srcDesc, FloatPointer src,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer destDesc, FloatPointer dest);
  @Cast(value = "bool") static native boolean activBackPropagate(
      @Cast(value = "cudnnActivationDescriptor_t*") Pointer activDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer srcDesc, FloatPointer src,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer srcDiffDesc, FloatPointer srcDiff,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer destDesc, FloatPointer dest,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer destDiffDesc, FloatPointer destDiff);
  @Cast(value = "bool") static native boolean lrnFeedForward(
      @Cast(value = "cudnnLRNDescriptor_t*") Pointer normDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer xDesc, FloatPointer x,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer yDesc, FloatPointer y);
  @Cast(value = "bool") static native boolean lrnBackPropagate(
      @Cast(value = "cudnnLRNDescriptor_t*") Pointer normDesc,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer yDesc, FloatPointer y,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dyDesc, FloatPointer dy,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer xDesc, FloatPointer x,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dxDesc, FloatPointer dx);
  @Cast(value = "bool") static native boolean activWithLossFeedForward(
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer xDesc, FloatPointer x,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer yDesc, FloatPointer y);
  @Cast(value = "bool") static native boolean activWithLossBackPropagate(
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer yDesc, FloatPointer y,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dyDesc, FloatPointer dy,
      @Cast(value = "cudnnTensorDescriptor_t*") Pointer dxDesc, FloatPointer dx);
}
