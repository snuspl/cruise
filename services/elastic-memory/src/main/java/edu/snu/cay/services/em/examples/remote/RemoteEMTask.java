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
package edu.snu.cay.services.em.examples.remote;

import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.evaluator.impl.OperationRouter;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Task code for testing remote access of memory store.
 * It assumes there are only two evaluators participating in EM service.
 * Task code invokes PUT/GET/REMOVE operations of memory store with two DATA_KEYs that belongs to one memory store.
 * The other memory store invokes PUT or REMOVE operations to update remote memory store state through remote access.
 * After then both memory stores invoke GET operation to confirm that the state of memory store is properly updated.
 */
final class RemoteEMTask implements Task {
  private static final Logger LOG = Logger.getLogger(RemoteEMTask.class.getName());

  private static final String DATA_TYPE = "INTEGER";
  private static final long DATA_KEY0 = 0;
  private static final long DATA_KEY1 = 1;
  private static final int DATA_VALUE0 = 1000;
  private static final int DATA_VALUE1 = 1001;

  private final MemoryStore<Long> memoryStore;

  /**
   * A router that is an internal component of EM. Here we use it in user code for testing purpose.
   */
  private final OperationRouter router;

  private final String taskId;

  private final AggregationSlave aggregationSlave;
  private final EvalSideMsgHandler msgHandler;
  private final SerializableCodec<String> codec;

  @Inject
  private RemoteEMTask(final MemoryStore<Long> memorystore,
                       final OperationRouter router,
                       final AggregationSlave aggregationSlave,
                       final EvalSideMsgHandler msgHandler,
                       final SerializableCodec<String> codec,
                       @Parameter(TaskConfigurationOptions.Identifier.class) final String taskId) {
    this.memoryStore = memorystore;
    this.router = router;
    this.aggregationSlave = aggregationSlave;
    this.msgHandler = msgHandler;
    this.codec = codec;
    this.taskId = taskId;
  }

  /**
   * Synchronize all tasks with a barrier in driver. Using this method, workers can share same view on stores for each
   * step.
   */
  private void synchronize() {
    aggregationSlave.send(RemoteEMDriver.AGGREGATION_CLIENT_ID, codec.encode(taskId));
    msgHandler.waitForMessage();
  }

  public byte[] call(final byte[] memento) throws InterruptedException, IdGenerationException {

    LOG.info("RemoteEMTask commencing...");

    final List<Long> keys = new LinkedList<>();
    keys.add(DATA_KEY0);
    keys.add(DATA_KEY1);

    final List<Integer> values = new LinkedList<>();
    values.add(DATA_VALUE0);
    values.add(DATA_VALUE1);

    Pair<Long, Integer> outputPair;
    Map<Long, Integer> outputMap;

    final List<LongRange> rangeList = new ArrayList<>(1);
    rangeList.add(new LongRange(0, 1));
    final boolean isLocalKey = !router.route(rangeList).getFirst().isEmpty();

    // 1. INITIAL STATE: check that the store does not contain DATA
    outputMap = memoryStore.getRange(DATA_TYPE, DATA_KEY0, DATA_KEY1);
    LOG.log(Level.INFO, "getRange({0}, {1}): {2}", new Object[]{DATA_KEY0, DATA_KEY1, outputMap});

    if (!outputMap.isEmpty()) {
      throw new RuntimeException("Wrong initial state");
    }

    synchronize();

    // 2. Put DATA into store via remote access
    // It should be performed by a memory store that does not own DATA_KEY.
    if (!isLocalKey) {
      final Map<Long, Boolean> putResult = memoryStore.putList(DATA_TYPE, keys, values);

      LOG.log(Level.INFO, "putList({0}, {1}): {2}", new Object[]{keys, values, putResult});
      for (final Map.Entry<Long, Boolean> entry : putResult.entrySet()) {
        if (!entry.getValue()) {
          throw new RuntimeException("Fail to put data");
        }
      }
    }

    synchronize();

    // 3. AFTER PUT: check that all workers can get DATA from the store
    outputPair = memoryStore.get(DATA_TYPE, DATA_KEY0);
    LOG.log(Level.INFO, "get({0}): {1}", new Object[]{DATA_KEY0, outputPair});

    if (outputPair == null) {
      throw new RuntimeException("Fail to get data");
    }
    if (outputPair.getFirst() != DATA_KEY0 || outputPair.getSecond() != DATA_VALUE0) {
      throw new RuntimeException("Fail to get correct data");
    }

    outputPair = memoryStore.get(DATA_TYPE, DATA_KEY1);
    LOG.log(Level.INFO, "get({0}): {1}", new Object[]{DATA_KEY1, outputPair});

    if (outputPair == null) {
      throw new RuntimeException("Fail to get data");
    }
    if (outputPair.getFirst() != DATA_KEY1 || outputPair.getSecond() != DATA_VALUE1) {
      throw new RuntimeException("Fail to get correct data");
    }

    outputMap = memoryStore.getRange(DATA_TYPE, 0L, 1L);
    LOG.log(Level.INFO, "getRange({0}, {1}): {2}", new Object[]{DATA_KEY0, DATA_KEY1, outputMap});

    if (!outputMap.containsKey(DATA_KEY0) || !outputMap.containsKey(DATA_KEY1)) {
      throw new RuntimeException("Fail to get data");
    }
    if (!outputMap.get(DATA_KEY0).equals(DATA_VALUE0) || !outputMap.get(DATA_KEY1).equals(DATA_VALUE1)) {
      throw new RuntimeException("Fail to get correct data");
    }

    synchronize();

    // 4. Remove DATA from the store via remote access
    // It should be performed by a memory store that does not own DATA_KEY.
    if (!isLocalKey) {
      outputMap = memoryStore.removeRange(DATA_TYPE, DATA_KEY0, DATA_KEY1);
      LOG.log(Level.INFO, "removeRange({0}, {1}): {2}", new Object[]{DATA_KEY0, DATA_KEY1, outputMap});

      if (!outputMap.containsKey(DATA_KEY0) || !outputMap.containsKey(DATA_KEY1)) {
        throw new RuntimeException("Fail to remove data");
      }
      if (!outputMap.get(DATA_KEY0).equals(DATA_VALUE0) || !outputMap.get(DATA_KEY1).equals(DATA_VALUE1)) {
        throw new RuntimeException("Fail to remove correct data");
      }
    }

    synchronize();

    // 5. AFTER REMOVE: check that the store does not contain DATA
    outputMap = memoryStore.getRange(DATA_TYPE, DATA_KEY0, DATA_KEY1);
    LOG.log(Level.INFO, "getRange({0}, {1}): {2}", new Object[]{DATA_KEY0, DATA_KEY1, outputMap});

    if (!outputMap.isEmpty()) {
      throw new RuntimeException("RemoveRange did not work");
    }

    // last sync to make sure all evaluators are alive until the end of all remote operations
    synchronize();

    return null;
  }
}
