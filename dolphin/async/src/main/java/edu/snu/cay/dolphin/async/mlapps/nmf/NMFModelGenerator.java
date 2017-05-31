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

import edu.snu.cay.dolphin.async.mlapps.nmf.NMFParameters.*;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class that generates and deals with values used in Non Matrix Factorization.
 */
final class NMFModelGenerator {

  private static final Logger LOG = Logger.getLogger(NMFModelGenerator.class.getName());

  private final VectorFactory vectorFactory;
  private final Random random = new Random();
  private final int rank;
  private final Float initMax;
  private final Float initMin;
  private final Float maxVal;

  @Inject
  private NMFModelGenerator(final VectorFactory vectorFactory,
                            @Parameter(Rank.class) final int rank,
                            @Parameter(InitialMax.class) final float initMax,
                            @Parameter(InitialMin.class) final float initMin,
                            @Parameter(MaxValue.class) final Float maxVal) {
    this.vectorFactory = vectorFactory;
    this.rank = rank;
    this.initMax = initMax;
    this.initMin = initMin;
    this.maxVal = maxVal;
  }

  /**
   * Checks whether the value is valid. If not, this returns new valid value following specification.
   * @param value value to check validation
   * @return valid value
   */
  private float getValidValue(final float value) {
    // value should not be larger than maxVal to prevent overflow
    float newValue = value;
    if (newValue > maxVal) {
      LOG.log(Level.FINE,
          "Value {0} is greater than the max {1} and will be replaced with the max", new Object[]{newValue, maxVal});
      newValue = maxVal;
    }
    // non-negativity
    if (newValue < 0.0f) {
      LOG.log(Level.FINE,
          "Value {0} is less than zero and will be replaced with zero for non-negativity", newValue);
      newValue = 0.0f;
    }
    return newValue;
  }

  /**
   * Returns the vector in which all elements are valid. (in-place update)
   * An element is valid when it does not exceed the specified maximum value and not negative.
   * @param vec the vector to check the validity
   * @return a valid vector
   */
  Vector getValidVector(final Vector vec) {
    for (int i = 0; i < vec.length(); ++i) {
      vec.set(i, getValidValue(vec.get(i)));
    }
    return vec;
  }

  /**
   * Returns a new random vector with length {@link Rank}.
   * Elements are randomly generated in range [{@link InitialMin}, {@link InitialMax}].
   * @return newly generated random vector
   */
  Vector createRandomVector() {
    final float[] values = new float[rank];
    for (int i = 0; i < rank; ++i) {
      final float newValue = random.nextFloat() * (initMax - initMin) + initMin;
      values[i] = getValidValue(newValue);
    }
    return vectorFactory.createDense(values);
  }
}
