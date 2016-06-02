/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.bsp.examples.sleep;

import org.apache.reef.io.network.group.api.operators.Reduce;

import javax.inject.Inject;

/**
 * An irrelevant reduce function used for {@link SleepREEF}.
 */
public class SleepReduceFunction implements Reduce.ReduceFunction<Object> {

  private final Object object;

  @Inject
  public SleepReduceFunction() {
    this.object = new Object();
  }

  @Override
  public final Object apply(final Iterable<Object> dataList) {
    return object;
  }
}
