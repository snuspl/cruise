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
package edu.snu.cay.async.examples.mlr;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Random;

/**
 * {@link ParameterUpdater} for the MLRREEF application.
 * Simply adds delta vectors to the old vectors stored in this server.
 * Vectors are initialized with values drawn from the normal distribution.
 */
final class MLRUpdater implements ParameterUpdater<Integer, Vector, Vector> {

  private final int numFeatures;
  private final VectorFactory vectorFactory;
  private final Random random;

  @Inject
  private MLRUpdater(@Parameter(MLRREEF.NumFeatures.class) final int numFeatures,
                     final VectorFactory vectorFactory) {
    this.numFeatures = numFeatures;
    this.vectorFactory = vectorFactory;
    this.random = new Random();
  }

  @Override
  public Vector process(final Integer key, final Vector preValue) {
    return preValue;
  }

  @Override
  public Vector update(final Vector oldValue, final Vector deltaValue) {
    return oldValue.addi(deltaValue);
  }

  @Override
  public Vector initValue(final Integer key) {
    final double[] features = new double[numFeatures];
    for (int featureIndex = 0; featureIndex < numFeatures; featureIndex++) {
      features[featureIndex] = random.nextGaussian() * 0.01;
    }
    return vectorFactory.createDense(features);
  }
}
