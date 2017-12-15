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
package edu.snu.cay.dolphin.async.mlapps.mlr;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import edu.snu.cay.dolphin.async.DolphinParameters.*;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Random;


/**
 * An UpdateFunction for the MLR application.
 * Simply adds delta vectors to the old vectors stored in this server.
 * Vectors are initialized with values drawn from the normal distribution.
 */
public final class MLRETModelUpdateFunction implements UpdateFunction<Integer, Vector, Vector> {

  private final int numFeaturesPerPartition;
  private final double modelGaussian;
  private final VectorFactory vectorFactory;
  private final Random random;

  @Inject
  private MLRETModelUpdateFunction(@Parameter(NumFeaturesPerPartition.class) final int numFeaturesPerPartition,
                                   @Parameter(ModelGaussian.class) final double modelGaussian,
                                   final VectorFactory vectorFactory) {
    this.numFeaturesPerPartition = numFeaturesPerPartition;
    this.modelGaussian = modelGaussian;
    this.vectorFactory = vectorFactory;
    this.random = new Random();
  }

  @Override
  public Vector initValue(final Integer integer) {
    final float[] features = new float[numFeaturesPerPartition];
    for (int featureIndex = 0; featureIndex < numFeaturesPerPartition; featureIndex++) {
      features[featureIndex] = (float) (random.nextGaussian() * modelGaussian);
    }
    return vectorFactory.createDense(features);
  }

  @Override
  public Vector updateValue(final Integer integer, final Vector oldValue, final Vector deltaValue) {
    return oldValue.addi(deltaValue);
  }
}
