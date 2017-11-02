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
package edu.snu.cay.services.et.examples.tableaccess;

import edu.snu.cay.services.et.evaluator.api.UpdateFunction;

import javax.inject.Inject;

/**
 * Update function in table access example.
 * Add prefix to old value (Updated-).
 */
public final class PrefixUpdateFunction implements UpdateFunction<Long, String, String> {

  static final String EMPTY_INIT_VALUE = "EMPTY";
  static final String UPDATE_PREFIX = "Update-";

  @Inject
  private PrefixUpdateFunction() {

  }

  @Override
  public String initValue(final Long key) {
    return EMPTY_INIT_VALUE;
  }

  @Override
  public String updateValue(final Long key, final String oldValue, final String updateValue) {
    return updateValue + oldValue;
  }
}
