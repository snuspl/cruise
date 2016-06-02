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

import javax.inject.Inject;

/**
 * {@link ParameterUpdater} for the LassoREEF application.
 * Simply replace old values with new values.
 */
final class LassoUpdater implements ParameterUpdater<Integer, Double, Double> {

  @Inject
  private LassoUpdater() {
  }

  @Override
  public Double process(final Integer key, final Double preValue) {
    return preValue;
  }

  @Override
  public Double update(final Double oldValue, final Double newValue) {
    return newValue;
  }

  @Override
  public Double initValue(final Integer key) {
    return 0D;
  }
}
