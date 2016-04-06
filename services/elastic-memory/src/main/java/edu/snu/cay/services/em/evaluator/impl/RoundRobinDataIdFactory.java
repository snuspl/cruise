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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.common.parameters.NumPartitions;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Issues ids for the MemoryStore to access data locally at the first time.
 * To distribute the data evenly to partitions, it assigns ids in round-robin fashion.
 * This factory also guarantees that Partition p has the data whose key is within
 * [p * PARTITION_SIZE, (p+1) * PARTITION_SIZE).
 */
public final class RoundRobinDataIdFactory implements DataIdFactory<Long> {
  private final int memoryStoreId;
  private final int numPartitions;
  private final int numInitialEvals;

  @Inject
  private RoundRobinDataIdFactory(@Parameter(MemoryStoreId.class) final int memoryStoreId,
                                  @Parameter(NumPartitions.class) final int numPartitions,
                                  @Parameter(NumInitialEvals.class) final int numInitialEvals) {
    this.memoryStoreId = memoryStoreId;
    this.numPartitions = numPartitions;
    this.numInitialEvals = numInitialEvals;
  }

  /**
   * {@code counter} starts from 0, increases one by one
   * when we create data ids by calling {@code getId} and {@code getIds}.
   */
  private final AtomicLong counter = new AtomicLong(0);

  @Override
  public Long getId() throws IdGenerationException {
    final Long count = counter.getAndIncrement();

    // If the number of partitions is not divisible by the number of initial Evaluators,
    // the remainders should be taken by the corresponding Evaluators. For example,
    // when there are 23 partitions and 4 Evaluators, then the Evaluators take (6, 6, 6, 5) partitions respectively.
    final int numPartitionsInEval = numPartitions / numInitialEvals +
          ((numPartitions % numInitialEvals >= memoryStoreId) ? 1 : 0);

    final long partitionSize = Long.MAX_VALUE / numPartitions;
    final long offset =  count / numPartitionsInEval;
    if (offset >= partitionSize) {
      throw new IdGenerationException("The number of ids in one partition has exceeded its limit.");
    }
    final int partitionId = memoryStoreId + numInitialEvals * (count.intValue() % numPartitionsInEval);
    return partitionId * partitionSize + offset;
  }

  @Override
  public List<Long> getIds(final int size) throws IdGenerationException {
    final List<Long> ids = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      ids.add(getId());
    }
    return ids;
  }
}
