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
package edu.snu.cay.services.em.examples.simple;

import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.examples.simple.parameters.NumMoves;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

final class SimpleEMTask implements Task {
  private static final Logger LOG = Logger.getLogger(SimpleEMTask.class.getName());

  // As each MemoryStore has 10 blocks, each block will have 2 items with use of RoundRobinDataIdFactory
  private static final int NUM_DATA = 20;

  private final MemoryStore<Long> memoryStore;
  private final AggregationSlave aggregationSlave;
  private final Codec<String> codec;
  private final EvalSideMsgHandler msgHandler;
  private final int numMoves;

  /**
   * Keys to put/get the data with.
   */
  private final List<Long> ids;

  @Inject
  private SimpleEMTask(
      final MemoryStore<Long> memoryStore,
      final AggregationSlave aggregationSlave,
      final SerializableCodec<String> codec,
      final EvalSideMsgHandler msgHandler,
      final DataIdFactory<Long> dataIdFactory,
      @Parameter(NumMoves.class) final int numMoves) throws IdGenerationException {
    this.memoryStore = memoryStore;
    this.aggregationSlave = aggregationSlave;
    this.codec = codec;
    this.msgHandler = msgHandler;
    this.numMoves = numMoves;

    this.ids = dataIdFactory.getIds(NUM_DATA);
    // Just use ids as data (data do not matter)

    for (final long id : ids) {
      final Pair<Long, Boolean> data = memoryStore.put(id, id);
      if (!data.getSecond()) {
        throw new RuntimeException("Fail to put initial data");
      }
    }
  }

  public byte[] call(final byte[] memento) throws InterruptedException {
    // the initial number of local blocks
    int prevNumBlocks = memoryStore.getNumBlocks();

    LOG.info("SimpleEMTask commencing...");

    checkAllDataAccessible();

    for (int i = 0; i < numMoves; i++) {
      // tell driver that I'm ready
      aggregationSlave.send(SimpleEMDriver.AGGREGATION_CLIENT_ID, codec.encode(DriverSideMsgHandler.READY));
      LOG.log(Level.INFO, "Waiting for driver to finish {0}th move.", i + 1);

      // data is moving.

      // wait until move is completed by driver
      final long numChangedBlocks = msgHandler.waitForMessage();

      // check that the local block is matched with the result of move
      final int curNumBlocks = memoryStore.getNumBlocks();
      assert prevNumBlocks + numChangedBlocks == curNumBlocks;
      LOG.log(Level.INFO, "Move result: [prevBlocks: {0}, changedBlocks: {1}, currentBlocks: {2}]",
          new Object[]{prevNumBlocks, numChangedBlocks, curNumBlocks});
      prevNumBlocks = curNumBlocks;

      // Move is finished, but let's wait for the local routing table is correctly updated
      // It's an ad-hoc way, because the touting table is not guaranteed to be updated even after sleep
      Thread.sleep(1000);

      // Even after move(), all the loaded data must be same as original.
      checkAllDataAccessible();
    }

    // need to sync here in order to guarantee that all evaluators are alive until all remote operations are finished
    aggregationSlave.send(SimpleEMDriver.AGGREGATION_CLIENT_ID, codec.encode(DriverSideMsgHandler.READY));
    msgHandler.waitForMessage();

    LOG.info("Finishing SimpleEMTask...");
    return null;
  }

  /**
   * Check that all data is alive in the store.
   */
  private void checkAllDataAccessible() {
    for (final long id : ids) {
      final Pair<Long, Long> data = memoryStore.get(id);
      if (data == null) {
        throw new RuntimeException("Data entry is lost");
      }
      if (data.getSecond() != id) {
        throw new RuntimeException("Data value is contaminated");
      }
    }
  }
}
