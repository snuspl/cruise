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
package edu.snu.cay.services.et.examples.simple;

import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import org.apache.commons.lang3.NotImplementedException;

import javax.inject.Inject;

/**
 * A simple version of {@link UpdateFunction}.
 * It is only for providing an init value for {@link edu.snu.cay.services.et.evaluator.api.Table#getOrInit} method.
 */
final class SimpleUpdateFunction implements UpdateFunction<Long, String, String> {
  private static final String VALUE_PREFIX = "INIT_";

  @Inject
  private SimpleUpdateFunction() {
  }

  @Override
  public String initValue(final Long key) {
    return getInitValue(key);
  }

  @Override
  public String updateValue(final Long key, final String oldValue, final String updateValue) {
    throw new NotImplementedException("This method is not used in this example.");
  }

  static String getInitValue(final long key) {
    return VALUE_PREFIX + key;
  }
}
