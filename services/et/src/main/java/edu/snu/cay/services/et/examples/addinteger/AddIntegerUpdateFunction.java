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
package edu.snu.cay.services.et.examples.addinteger;

import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import edu.snu.cay.services.et.examples.addinteger.parameters.UpdateCoefficient;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A simple UpdateFunction that accumulates integers.
 */
public final class AddIntegerUpdateFunction implements UpdateFunction<Object, Integer, Integer> {
  private final int updateCoefficient;

  /**
   * Injectable constructor.
   */
  @Inject
  private AddIntegerUpdateFunction(@Parameter(UpdateCoefficient.class) final int updateCoefficient) {
    this.updateCoefficient = updateCoefficient;
  }

  @Override
  public Integer initValue(final Object key) {
    return 0;
  }

  @Override
  public Integer updateValue(final Object key, final Integer oldValue, final Integer deltaValue) {
    return oldValue + deltaValue * updateCoefficient;
  }
}
