/**
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

package edu.snu.reef.em.examples.simple;

import edu.snu.reef.em.evaluator.api.MemoryStore;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

final class SimpleEMTask implements Task {
  private static final Logger LOG = Logger.getLogger(SimpleEMTask.class.getName());
  public static final String KEY = "INTEGER";
  private static final Integer SLEEP_MILLISECONDS = 10000;

  private final MemoryStore memoryStore;
  private final SimpleEMTaskReady simpleEMTaskReady;
  private final HeartBeatTriggerManager heartBeatTriggerManager;

  @Inject
  private SimpleEMTask(
      final MemoryStore memoryStore,
      final SimpleEMTaskReady simpleEMTaskReady,
      final HeartBeatTriggerManager heartBeatTriggerManager) {
    this.memoryStore = memoryStore;
    this.simpleEMTaskReady = simpleEMTaskReady;
    this.heartBeatTriggerManager = heartBeatTriggerManager;

    final List<Integer> list = new LinkedList<>();
    list.add(100);
    list.add(200);

    this.memoryStore.putMovable(KEY, list);
  }

  public byte[] call(final byte[] memento) throws InterruptedException {
    LOG.info("SimpleEMTask commencing...");

    LOG.info("Before sleep, memory store contains: ");
    LOG.info(memoryStore.get(KEY).toString());
    // Should be [100, 200]

    simpleEMTaskReady.setReady(true);
    heartBeatTriggerManager.triggerHeartBeat();
    Thread.sleep(SLEEP_MILLISECONDS);

    LOG.info("After sleep, memory store contains: ");
    LOG.info(memoryStore.get(KEY).toString());
    // Fast evaluator should be [100, 200, 100, 200]
    // Slow evaluator should be []

    return null;
  }
}
