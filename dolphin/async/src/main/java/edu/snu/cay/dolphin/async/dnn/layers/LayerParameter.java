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
package edu.snu.cay.dolphin.async.dnn.layers;

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;

/**
 * The parameter of the layer.
 */
public final class LayerParameter {
  private final Matrix weightParam;
  private final Matrix biasParam;

  /**
   * Generates a new instance of a layer parameter.
   * @param matrixFactory a factory to create the matrices that the layer parameter contains
   * @return the generated empty layer parameter
   */
  public static LayerParameter newEmptyInstance(final MatrixFactory matrixFactory) {
    return new LayerParameter(matrixFactory.create(0), matrixFactory.create(0));
  }

  /**
   * @return a new LayerParameter builder.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * @return the weight matrix of the parameter.
   */
  public Matrix getWeightParam() {
    return weightParam;
  }

  /**
   * @return the bias vector of the parameter.
   */
  public Matrix getBiasParam() {
    return biasParam;
  }

  public static final class Builder implements org.apache.reef.util.Builder<LayerParameter> {
    private Matrix weightParam;
    private Matrix biasParam;

    public Builder setWeightParam(final Matrix weightParam) {
      this.weightParam = weightParam;
      return this;
    }

    public Builder setBiasParam(final Matrix biasParam) {
      this.biasParam = biasParam;
      return this;
    }

    @Override
    public LayerParameter build() {
      return new LayerParameter(this.weightParam, this.biasParam);
    }
  }

  private LayerParameter(final Matrix weightParam,
                         final Matrix biasParam) {
    this.weightParam = weightParam;
    this.biasParam = biasParam;
  }

  @Override
  public String toString() {
    return "weight: " + weightParam.toString() + ", bias: " + biasParam.toString();
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof LayerParameter)) {
      return false;
    }

    final LayerParameter other = (LayerParameter)obj;
    return weightParam.equals(other.weightParam) && biasParam.equals(other.biasParam);
  }

  @Override
  public int hashCode() {
    final int weightParamHashCode = weightParam == null ? 0 : weightParam.hashCode();
    final int biasParamHashCode = biasParam == null ? 0 : biasParam.hashCode();
    return weightParamHashCode * 31 + biasParamHashCode;
  }
}
