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

import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.evaluator.api.PartitionTracker;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import edu.snu.cay.services.em.examples.simple.parameters.Iterations;
import edu.snu.cay.services.em.examples.simple.parameters.PeriodMillis;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

final class SimpleEMTask implements Task {
  private static final Logger LOG = Logger.getLogger(SimpleEMTask.class.getName());
  static final String DATATYPE = "INTEGER";
  private static final int NUM_DATA = 20;

  private final MemoryStore memoryStore;
  private final SimpleEMTaskReady simpleEMTaskReady;
  private final HeartBeatTriggerManager heartBeatTriggerManager;
  private final int iterations;
  private final long periodMillis;

  private final List<Long> ids;

  @Inject
  private SimpleEMTask(
      final MemoryStore memoryStore,
      final SimpleEMTaskReady simpleEMTaskReady,
      final HeartBeatTriggerManager heartBeatTriggerManager,
      final PartitionTracker partitionTracker,
      final DataIdFactory<Long> dataIdFactory,
      @Parameter(Iterations.class) final int iterations,
      @Parameter(PeriodMillis.class) final long periodMillis) throws IdGenerationException {
    this.memoryStore = memoryStore;
    this.simpleEMTaskReady = simpleEMTaskReady;
    this.heartBeatTriggerManager = heartBeatTriggerManager;
    this.iterations = iterations;
    this.periodMillis = periodMillis;

    this.ids = dataIdFactory.getIds(NUM_DATA);
    // Just use ids as data (data do not matter)
    this.memoryStore.putList(DATATYPE, ids, ids);

    partitionTracker.registerPartition(DATATYPE, ids.get(0), ids.get(NUM_DATA - 1));
  }

  public byte[] call(final byte[] memento) throws InterruptedException {
    LOG.info("SimpleEMTask commencing...");

    LOG.log(Level.INFO, "Before sleep, memory store contains {0} blocks", memoryStore.getAll(DATATYPE));

    final Map<Long, Object> initialData = memoryStore.getRange(DATATYPE, ids.get(0), ids.get(NUM_DATA - 1));
    assert initialData.keySet().containsAll(ids);
    assert initialData.values().containsAll(ids);

    simpleEMTaskReady.setReady(true);
    heartBeatTriggerManager.triggerHeartBeat();

    // Sleep one more period than the Driver, to give the Driver time to finish
    final long sleepMillis = (iterations + 1) * periodMillis;
    LOG.info("Sleep for: " + sleepMillis);
    Thread.sleep(sleepMillis);

    LOG.log(Level.INFO, "After sleep, memory store contains {0} blocks", memoryStore.getAll(DATATYPE));
    final Map<Long, Object> dataAfterMove = memoryStore.getRange(DATATYPE, ids.get(0), ids.get(NUM_DATA - 1));
    assert dataAfterMove.keySet().containsAll(ids); // The same data must be loaded even after moved.
    assert dataAfterMove.values().containsAll(ids);

    return null;
  }
}
