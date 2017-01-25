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
package edu.snu.cay.dolphin.async.mlapps.lasso;

import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Random;

import static edu.snu.cay.dolphin.async.mlapps.lasso.LassoParameters.ModelGaussian;

/**
 * {@link ParameterUpdater} for the LassoREEF application.
 * Simply adds delta vectors to the old vectors stored in this server.
 * Vectors are initialized with values drawn from the normal distribution.
 */
final class LassoUpdater implements ParameterUpdater<Integer, Double, Double> {

  private final double modelGaussian;
  private final Random random;

  @Inject
  private LassoUpdater(@Parameter(ModelGaussian.class) final double modelGaussian) {
    this.modelGaussian = modelGaussian;
    this.random = new Random();
  }

  @Override
  public Double process(final Integer key, final Double preValue) {
    return preValue;
  }

  @Override
  public Double update(final Double oldValue, final Double deltaValue) {
    return oldValue + deltaValue;
  }

  @Override
  public Double initValue(final Integer key) {
    return random.nextGaussian() * modelGaussian;
  }
}
