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
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Task code for testing remote access of memory store.
 * It assumes there are only two evaluators participating in EM service.
 * Task code invokes PUT/GET/REMOVE operations of memory store with a single DATA_KEY that belongs to one memory store.
 * The other memory store invokes PUT or REMOVE operations to update remote memory store state through remote access.
 * After then both memory stores invoke GET operation to confirm that the state of memory store is properly updated.
 */
final class RemoteEMTask implements Task {
  private static final Logger LOG = Logger.getLogger(RemoteEMTask.class.getName());

  private static final String DATA_TYPE = "INTEGER";
  private static final long DATA_KEY = 0;
  private static final int DATA_VALUE = 1000;

  private final MemoryStore memoryStore;

  /**
   * A router that is an internal component of EM.
   * Here we use it in user code for testing purpose.
   */
  private final OperationRouter router;

  private final String taskId;

  private final AggregationSlave aggregationSlave;
  private final EvalSideMsgHandler msgHandler;
  private final SerializableCodec<String> codec;

  @Inject
  private RemoteEMTask(final MemoryStore memorystore,
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
   * Synchronize all tasks with a barrier in driver.
   * Using this method, workers can share same view on stores for each step.
   */
  private void synchronize() {
    aggregationSlave.send(RemoteEMDriver.AGGREGATION_CLIENT_ID, codec.encode(taskId));
    msgHandler.waitForMessage();
  }

  public byte[] call(final byte[] memento) throws InterruptedException, IdGenerationException {

    LOG.info("RemoteEMTask commencing...");

    final boolean isLocalKey = router.route(DATA_KEY).getFirst();

    boolean isSuccess;
    Pair<Long, Integer> output;

    // 1. INITIAL STATE: check that the store does not contain DATA
    output = memoryStore.get(DATA_TYPE, DATA_KEY);
    LOG.log(Level.INFO, "get({0}): {1}", new Object[]{DATA_KEY, output});

    if (output != null) {
      throw new RuntimeException("Wrong initial state");
    }

    synchronize();

    // 2. Put DATA into store via remote access
    // It should be performed by a memory store that does not own DATA_KEY.
    if (!isLocalKey) {
      isSuccess = memoryStore.put(DATA_TYPE, DATA_KEY, DATA_VALUE);
      LOG.log(Level.INFO, "put({0}): {1}", new Object[]{DATA_KEY, isSuccess});

      if (!isSuccess) {
        throw new RuntimeException("Fail to put data");
      }
    }

    synchronize();

    // 3. AFTER PUT: check that all workers can get DATA from the store
    output = memoryStore.get(DATA_TYPE, DATA_KEY);
    LOG.log(Level.INFO, "get({0}): {1}", new Object[]{DATA_KEY, output});

    if (output == null) {
      throw new RuntimeException("Fail to get data");
    }
    if (output.getFirst() != DATA_KEY || output.getSecond() != DATA_VALUE) {
      throw new RuntimeException("Fail to get correct data");
    }

    synchronize();

    // 4. Remove DATA from store via remote access
    // It should be performed by a memory store that does not own DATA_KEY.
    if (!isLocalKey) {
      output = memoryStore.remove(DATA_TYPE, DATA_KEY);
      LOG.log(Level.INFO, "remove({0}): {1}", new Object[]{DATA_KEY, output});

      if (output == null) {
        throw new RuntimeException("Fail to remove data");
      }
      if (output.getFirst() != DATA_KEY || output.getSecond() != DATA_VALUE) {
        throw new RuntimeException("Fail to remove correct data");
      }
    }

    synchronize();

    // 5. AFTER REMOVE: check that the store does not contain DATA
    output = memoryStore.get(DATA_TYPE, DATA_KEY);
    LOG.log(Level.INFO, "get({0}): {1}", new Object[]{DATA_KEY, output});

    if (output != null) {
      throw new RuntimeException("Remove did not work well");
    }

    // last sync to make sure all evaluators are alive until the end of all remote operations
    synchronize();

    return null;
  }
}
