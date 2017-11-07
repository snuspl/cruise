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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.evaluator.api.UpdateFunction;

import javax.inject.Inject;

/**
 * An empty implementation of UpdateFunction.
 */
public final class VoidUpdateFunction implements UpdateFunction<Void, Void, Void> {

  /**
   * Injectable constructor.
   */
  @Inject
  private VoidUpdateFunction() {
  }

  @Override
  public Void initValue(final Void key) {
    return null;
  }

  @Override
  public Void updateValue(final Void key, final Void oldValue, final Void deltaValue) {
    return null;
  }
}
