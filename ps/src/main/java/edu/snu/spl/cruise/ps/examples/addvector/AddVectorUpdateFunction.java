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
package edu.snu.spl.cruise.ps.examples.addvector;

import edu.snu.spl.cruise.common.math.linalg.Vector;
import edu.snu.spl.cruise.common.math.linalg.VectorFactory;
import edu.snu.spl.cruise.services.et.evaluator.api.UpdateFunction;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * {@link UpdateFunction} for the AddIntegerREEF application.
 * Simply adds all incoming integer values to all elements of the vector
 * (see {@link AddVectorUpdateFunction#updateValue(Integer, Vector, Integer)}).
 * The initial element in vector of every distinct key is set to be 0
 * (see {@link AddVectorUpdateFunction#initValue(Integer)}).
 */
final class AddVectorUpdateFunction implements UpdateFunction<Integer, Vector, Integer> {

  private final int vectorSize;
  private final VectorFactory vectorFactory;

  @Inject
  private AddVectorUpdateFunction(@Parameter(AddVectorET.VectorSize.class) final int vectorSize,
                                  final VectorFactory vectorFactory) {
    this.vectorSize = vectorSize;
    this.vectorFactory = vectorFactory;
  }

  @Override
  public Vector initValue(final Integer key) {
    final float[] vector = new float[vectorSize];
    for (int vectorIdx = 0; vectorIdx < vectorSize; vectorIdx++) {
      vector[vectorIdx] = 0f;
    }
    return vectorFactory.createDense(vector);
  }

  @Override
  public Vector updateValue(final Integer key, final Vector oldValue, final Integer updateValue) {
    final float[] vector = new float[vectorSize];
    for (int vectorIdx = 0; vectorIdx < vectorSize; vectorIdx++) {
      vector[vectorIdx] = (float) updateValue;
    }

    return oldValue.addi(vectorFactory.createDense(vector));
  }
}
