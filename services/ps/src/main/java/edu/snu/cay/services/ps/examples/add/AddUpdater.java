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
package edu.snu.cay.services.ps.examples.add;

import edu.snu.cay.services.ps.server.api.ParameterUpdater;

import javax.inject.Inject;

/**
 * A ParameterUpdater that add integers. The initial value is 0, and updates
 * are applied as oldValue + deltaValue.
 */
public final class AddUpdater implements ParameterUpdater<Integer, Integer, Integer> {

  @Inject
  private AddUpdater() {
  }

  @Override
  public Integer process(final Integer key, final Integer preValue) {
    return preValue;
  }

  @Override
  public Integer update(final Integer oldValue, final Integer deltaValue) {
    return oldValue + deltaValue;
  }

  @Override
  public Integer initValue(final Integer key) {
    return 0;
  }
}
