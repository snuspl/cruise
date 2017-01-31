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

import javax.inject.Inject;

/**
 * A class that generates local keys based on the assigned block Ids.
 * TODO #26: provide a way to load data into local blocks
 */
public final class LocalKeyGenerator {

  public static final long SAMPLE_KEY = 0L;

  @Inject
  private LocalKeyGenerator() {

  }

  long get() {
    return SAMPLE_KEY;
  }
}
