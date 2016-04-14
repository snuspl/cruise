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
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Issues ids for the MemoryStore to access data locally at the first time.
 * To distribute the data evenly to blocks, it assigns ids in round-robin fashion.
 * This factory also guarantees that Block b has the data whose key is within
 * [b * BLOCK_SIZE, (p+1) * BLOCK_SIZE).
 */
public final class RoundRobinDataIdFactory implements DataIdFactory<Long> {
  private final int memoryStoreId;
  private final int numTotalBlocks;
  private final int numInitialEvals;

  @Inject
  private RoundRobinDataIdFactory(@Parameter(MemoryStoreId.class) final int memoryStoreId,
                                  @Parameter(NumTotalBlocks.class) final int numTotalBlocks,
                                  @Parameter(NumInitialEvals.class) final int numInitialEvals) {
    this.memoryStoreId = memoryStoreId;
    this.numTotalBlocks = numTotalBlocks;
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

    // If the number of blocks is not divisible by the number of initial Evaluators,
    // the remainders should be taken by the corresponding Evaluators. For example,
    // when there are 23 blocks and 4 Evaluators, then the Evaluators take (6, 6, 6, 5) blocks respectively.
    final int numBlocksInEval = numTotalBlocks / numInitialEvals +
          ((numTotalBlocks % numInitialEvals > memoryStoreId) ? 1 : 0);

    // Whenever getId() is called, one block is chosen in round-robin.
    final int blockId = memoryStoreId + numInitialEvals * (count.intValue() % numBlocksInEval);

    // A key (p * blockSize + offset) is issued, where p is id of the chosen block
    // and block p contains keys within the range of [p * blockSize, (p+1) * blockSize).
    final long blockSize = Long.MAX_VALUE / numTotalBlocks;
    final long offset = count / numBlocksInEval;
    if (offset >= blockSize) {
      throw new IdGenerationException("The number of ids in one block has exceeded its limit.");
    }
    return blockId * blockSize + offset;
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
