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
package edu.snu.cay.dolphin.async.examples.addvector;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * {@link ParameterUpdater} for the AddIntegerREEF application.
 * Simply adds all incoming integer values to all elements of the vector
 * (see {@link AddVectorUpdater#update(Vector, Vector)}).
 * The initial element in vector of every distinct key is set to be 0 (see {@link AddVectorUpdater#initValue(Integer)}).
 */
final class AddVectorUpdater implements ParameterUpdater<Integer, Integer, Vector> {

  private final int vectorSize;
  private final VectorFactory vectorFactory;

  @Inject
  private AddVectorUpdater(@Parameter(AddVectorREEF.VectorSize.class) final int vectorSize,
                           final VectorFactory vectorFactory) {
    this.vectorSize = vectorSize;
    this.vectorFactory = vectorFactory;
  }

  @Override
  public Vector process(final Integer key, final Integer preValue) {
    final double[] vector = new double[vectorSize];
    for (int vectorIdx = 0; vectorIdx < vectorSize; vectorIdx++) {
      vector[vectorIdx] = preValue;
    }
    return vectorFactory.createDense(vector);
  }

  @Override
  public Vector update(final Vector oldValue, final Vector deltaValue) {
    return oldValue.addi(deltaValue);
  }

  @Override
  public Vector initValue(final Integer key) {
    final double[] vector = new double[vectorSize];
    for (int vectorIdx = 0; vectorIdx < vectorSize; vectorIdx++) {
      vector[vectorIdx] = 0;
    }
    return vectorFactory.createDense(vector);
  }
}
