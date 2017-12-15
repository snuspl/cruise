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
package edu.snu.cay.dolphin.async.examples.addinteger;

import edu.snu.cay.services.et.evaluator.api.UpdateFunction;

import javax.inject.Inject;

/**
 * {@link UpdateFunction} for the AddIntegerREEF application.
 * Simply adds all incoming values (see {@link AddIntegerUpdater#update(Integer, Integer)}).
 * The initial sum for every distinct key is set to be 0 (see {@link AddIntegerUpdater#initValue(Integer)}).
 */
public final class AddIntegerUpdateFunction implements UpdateFunction<Integer, Integer, Integer> {

  @Inject
  private AddIntegerUpdateFunction() {
  }

  @Override
  public Integer initValue(final Integer key) {
    return 0;
  }

  @Override
  public Integer updateValue(final Integer key, final Integer oldValue, final Integer updateValue) {
    return oldValue + updateValue;
  }
}
