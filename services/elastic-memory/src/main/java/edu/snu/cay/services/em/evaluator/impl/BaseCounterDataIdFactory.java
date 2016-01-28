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
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An implementation of {@link DataIdFactory}. This factory creates ids of {@code Long} type
 * by combining {@code base} and {@code counter}.
 *
 * To guarantee that different factories create unique data ids without asking to the driver or
 * to other evaluators, the driver should bind a unique NamedParameter {@code Base} to each factory.
 *
 * Note that this factory can only create up to 2^{@code partitionSizeBits} global unique ids.
 * Beyond that, the methods throw Exceptions, because the ids are not guaranteed to be unique.
 */
public final class BaseCounterDataIdFactory implements DataIdFactory<Long> {

  /**
   * {@code counter} starts from 0, increases one by one
   * when we create data ids by calling {@code getId} and {@code getIds}.
   */
  private final AtomicLong counter = new AtomicLong(0);

  /**
   * {@code base} should be a global unique value.
   * Postfix of {@code taskId} in Dolphin and SimpleEMREEF is a possible value for {@code base}.
   */
  private final long base;

  /**
   * A number of bit representing the partition size.
   */
  private final int partitionSizeBits;

  @Inject
  private BaseCounterDataIdFactory(@Parameter(Base.class) final Integer base,
                                   @Parameter(RangePartitionFunc.PartitionSizeBits.class) final int partitionSizeBits) {
    this.base = (long) base << partitionSizeBits;
    this.partitionSizeBits = partitionSizeBits;
  }

  @Override
  public Long getId() throws IdGenerationException {
    if (counter.get() == (long) 1 << partitionSizeBits) {
      throw new IdGenerationException("No more id available");
    }
    return base + counter.getAndIncrement();
  }

  @Override
  public List<Long> getIds(final int size) throws IdGenerationException {
    if (counter.get() + size > (long) 1 << partitionSizeBits) {
      throw new IdGenerationException("No more id available");
    }
    final Vector<Long> idVector = new Vector<>();
    final long headId = counter.getAndAdd(size);
    for (int i = 0; i < size; i++) {
      idVector.add(base + headId + i);
    }
    return idVector;
  }

  @NamedParameter(doc = "Global unique base value of BaseCounterDataIdFactory to prevent conflict between each other")
  public final class Base implements Name<Integer> {
  }
}
