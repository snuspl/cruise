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

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.async.examples.nmf.NMFParameters.*;
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
  private final double initMax;
  private final double initMin;
  private final double maxVal;

  @Inject
  private NMFModelGenerator(final VectorFactory vectorFactory,
                            @Parameter(Rank.class) final int rank,
                            @Parameter(InitialMax.class) final double initMax,
                            @Parameter(InitialMin.class) final double initMin,
                            @Parameter(MaxValue.class) final double maxVal) {
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
  private double getValidValue(final double value) {
    // value should not be larger than maxVal to prevent overflow
    double newValue = value;
    if (newValue > maxVal) {
      LOG.log(Level.FINE,
          "Value {0} is greater than the max {1} and will be replaced with the max", new Object[]{newValue, maxVal});
      newValue = maxVal;
    }
    // non-negativity
    if (newValue < 0.0D) {
      LOG.log(Level.FINE,
          "Value {0} is less than zero and will be replaced with zero for non-negativity", newValue);
      newValue = 0.0D;
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
    final double[] values = new double[rank];
    for (int i = 0; i < rank; ++i) {
      final double newValue = random.nextDouble() * (initMax - initMin) + initMin;
      values[i] = getValidValue(newValue);
    }
    return vectorFactory.createDense(values);
  }
}
