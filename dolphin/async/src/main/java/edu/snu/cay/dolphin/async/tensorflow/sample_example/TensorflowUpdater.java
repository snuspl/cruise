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
package edu.snu.cay.dolphin.async.tensorflow.sample_example;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;

import javax.inject.Inject;

/**
 * {@link ParameterUpdater} for the TensorflowREEF application.
 * Simply adds all incoming integer values to all elements of the vector
 * (see {@link TensorflowUpdater#update(Vector, Vector)}).
 * The initial element in vector of every distinct key is set to be 0
 * (see {@link TensorflowUpdater#initValue(Integer)}).
 */
final class TensorflowUpdater implements ParameterUpdater<Integer, Vector, Vector> {

  private final VectorFactory vectorFactory;

  @Inject
  private TensorflowUpdater(final VectorFactory vectorFactory) {
    this.vectorFactory = vectorFactory;
  }

  @Override
  public Vector process(final Integer key, final Vector preValue) {
    return preValue;
  }

  @Override
  public Vector update(final Vector oldValue, final Vector deltaValue) {
    return deltaValue;
  }

  @Override
  public Vector initValue(final Integer key) {
    final double[] vector = new double[2];
    for (int vectorIdx = 0; vectorIdx < 2; vectorIdx++) {
      vector[vectorIdx] = 0;
    }
    return vectorFactory.createDense(vector);
  }
}
