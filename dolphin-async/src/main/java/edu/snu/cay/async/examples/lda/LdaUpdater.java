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
package edu.snu.cay.async.examples.lda;

import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 *
 */
final class LdaUpdater implements ParameterUpdater<Integer, int[], int[]> {

  private final int numTopics;

  @Inject
  private LdaUpdater(@Parameter(LdaREEF.NumTopics.class) final int numTopics) {
    this.numTopics = numTopics;
  }

  @Override
  public int[] process(final Integer key, final int[] preValue) {
    return preValue;
  }

  @Override
  public int[] update(final int[] oldValue, final int[] deltaValue) {
    // deltaValue have the position and value of the change.
    oldValue[deltaValue[0]] += deltaValue[1];
    return oldValue;
  }

  @Override
  public int[] initValue(final Integer key) {
    return new int[numTopics];
  }
}
