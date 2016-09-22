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
package edu.snu.cay.dolphin.async.mlapps.nmf;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

import static edu.snu.cay.dolphin.async.mlapps.nmf.NMFParameters.*;

/**
 * Updater for non-negative matrix factorization via SGD.
 *
 * Vectors are initialized with random values
 * between {@link InitialMin} and {@link InitialMax} using {@link java.util.Random}.
 */
final class NMFUpdater implements ParameterUpdater<Integer, Vector, Vector> {

  private final NMFModelGenerator modelGenerator;
  private final double stepSize;

  @Inject
  private NMFUpdater(final NMFModelGenerator modelGenerator,
                     @Parameter(StepSize.class) final double stepSize) {
    this.modelGenerator = modelGenerator;
    this.stepSize = stepSize;
  }

  @Override
  public Vector process(final Integer key, final Vector preValue) {
    return preValue;
  }

  @Override
  public Vector update(final Vector oldValue, final Vector deltaValue) {
    final Vector newVec = oldValue.axpy(-stepSize, deltaValue);
    // assume that all vectors are dense vectors
    return modelGenerator.getValidVector(newVec);
  }

  @Override
  public Vector initValue(final Integer key) {
    return modelGenerator.createRandomVector();
  }

  @Override
  public Vector aggregate(final Vector oldPreValue, final Vector newPreValue) {
    return oldPreValue.addi(newPreValue);
  }
}
