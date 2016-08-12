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
package edu.snu.cay.services.em.examples.remote;

import edu.snu.cay.services.em.evaluator.api.EMUpdateFunction;

import javax.inject.Inject;

/**
 * An implementation of {@link EMUpdateFunction} that accumulates the delta starting from zero.
 */
final class EMUpdateFunctionImpl implements EMUpdateFunction<Long, Integer> {

  /**
   * Injectable constructor.
   */
  @Inject
  private EMUpdateFunctionImpl() {
  }

  @Override
  public Integer getInitValue(final Long key) {
    return 0;
  }

  @Override
  public Integer getUpdatedValue(final Integer oldValue, final Integer deltaValue) {
    return oldValue + deltaValue;
  }
}
