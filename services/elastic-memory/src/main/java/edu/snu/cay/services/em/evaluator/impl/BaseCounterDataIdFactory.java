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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link DataIdFactory} implementation.
 * Gives {@code Long} type ids for evaluators.
 * All data ids are composed of {@code base} and {@code counter}.
 * Different BaseCounterDataIdFactories should have different {@code bases}
 * to guarantee that they create globally unique data ids without asking the driver.
 * Driver should bind a unique {@code NamedParameter} to initialize {@code base}.
 * Note that this factory has limitation in number of items: only provides 2^32 globally unique ids.
 */
public final class BaseCounterDataIdFactory implements DataIdFactory<Long> {

  /**
   * {@code counter} is an {@code AtomicInteger}.
   * Starts from 0, increases one by one when we create data ids by calling {@code getId} and {@code getIds}.
   */
  private final AtomicInteger counter = new AtomicInteger(0);

  /**
   * {@code base} should be a global unique value.
   * Postfix of {@code taskId} in Dolphin and SimpleEMREEF is a possible value for {@code base}.
   */
  private final long base;

  @Inject
  private BaseCounterDataIdFactory(@Parameter(Base.class) final Integer base) {
    this.base = (long) base << 32;
  }

  @Override
  public Long getId() {
    return base + counter.getAndIncrement();
  }

  @Override
  public List<Long> getIds(final int size) {
    final Vector<Long> idVector = new Vector<>();
    for (int i = 0; i < size; i++) {
      idVector.add(base + counter.getAndIncrement());
    }
    return idVector;
  }

  @NamedParameter(doc = "Global unique base value of BaseCounterDataIdFactory to prevents conflict between each other")
  public final class Base implements Name<Integer> {
  }
}
