/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.spl.cruise.ps.mlapps.nmf;

import edu.snu.spl.cruise.common.math.linalg.Vector;
import edu.snu.spl.cruise.ps.CruisePSParameters;
import edu.snu.spl.cruise.services.et.evaluator.api.UpdateFunction;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

import static edu.snu.spl.cruise.ps.mlapps.nmf.NMFParameters.InitialMax;
import static edu.snu.spl.cruise.ps.mlapps.nmf.NMFParameters.InitialMin;

/**
 * An update function for non-negative matrix factorization via SGD.
 *
 * Vectors are initialized with random values
 * between {@link InitialMin} and {@link InitialMax} using {@link java.util.Random}.
 */
public final class NMFModelUpdateFunction implements UpdateFunction<Integer, Vector, Vector> {
  private final NMFModelGenerator modelGenerator;
  private final float stepSize;

  @Inject
  private NMFModelUpdateFunction(final NMFModelGenerator modelGenerator,
                                 @Parameter(CruisePSParameters.StepSize.class) final float stepSize) {
    this.modelGenerator = modelGenerator;
    this.stepSize = stepSize;
  }

  @Override
  public Vector initValue(final Integer key) {
    return modelGenerator.createRandomVector();
  }

  @Override
  public Vector updateValue(final Integer key, final Vector oldValue, final Vector deltaValue) {
    final Vector newVec = oldValue.axpy(-stepSize, deltaValue);
    // assume that all vectors are dense vectors
    return modelGenerator.getValidVector(newVec);
  }
}
