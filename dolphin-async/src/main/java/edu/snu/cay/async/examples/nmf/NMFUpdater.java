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
package edu.snu.cay.async.examples.nmf;

import edu.snu.cay.async.examples.nmf.NMFParameters.*;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;

import javax.inject.Inject;

/**
 * Updater for non-negative matrix factorization via SGD.
 *
 * Vectors are initialized with random values
 * between {@link InitialMin} and {@link InitialMax} using {@link java.util.Random}.
 */
final class NMFUpdater implements ParameterUpdater<Integer, Vector, Vector> {

  private final NMFModelGenerator modelGenerator;

  @Inject
  private NMFUpdater(final NMFModelGenerator modelGenerator) {
    this.modelGenerator = modelGenerator;
  }

  @Override
  public Vector process(final Integer key, final Vector preValue) {
    return preValue;
  }

  @Override
  public Vector update(final Vector oldValue, final Vector deltaValue) {
    final Vector newVec = oldValue.subi(deltaValue);
    // assume that all vectors are dense vectors
    return modelGenerator.getValidVector(newVec);
  }

  @Override
  public Vector initValue(final Integer key) {
    return modelGenerator.createRandomVector();
  }
}
