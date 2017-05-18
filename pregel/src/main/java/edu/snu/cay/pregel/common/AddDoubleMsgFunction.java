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
package edu.snu.cay.pregel.common;

import com.google.common.collect.Lists;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;

import javax.inject.Inject;
import java.util.List;

/**
 * A simple UpdateFunction that accumulates Doubles.
 */
public final class AddDoubleMsgFunction implements UpdateFunction<Object, List<Double>, Double> {

  @Inject
  private AddDoubleMsgFunction() {

  }

  @Override
  public List<Double> initValue(final Object key) {
    return Lists.newArrayList();
  }

  @Override
  public List<Double> updateValue(final Object key, final List<Double> list, final Double msgValue) {
    list.add(msgValue);
    return list;
  }
}
