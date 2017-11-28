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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.avro.TaskletStatusType;
import edu.snu.cay.services.et.evaluator.api.MessageSender;
import edu.snu.cay.services.et.evaluator.api.Tasklet;
import edu.snu.cay.services.et.evaluator.api.TaskletMsgHandler;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by xyzi on 25/11/2017.
 */
public final class TaskletRuntime {
  private static final Logger LOG = Logger.getLogger(TaskletRuntime.class.getName());
  private static final int NUM_TASKLETS = 4;
  private final Injector taskletBaseInjector;
  private final MessageSender msgSender;

  private boolean closeFlag = false;

  private final Map<String, Pair<Tasklet, TaskletMsgHandler>> taskletMap = new ConcurrentHashMap<>();

  private final ExecutorService executorService = Executors.newFixedThreadPool(NUM_TASKLETS);

  @Inject
  private TaskletRuntime(final Injector taskletBaseInjector,
                         final MessageSender msgSender) {
    this.taskletBaseInjector = taskletBaseInjector;
    this.msgSender = msgSender;
  }

  public void startTasklet(final String taskletId, final Configuration taskletConf) throws InjectionException {
    synchronized (this) {
      if (closeFlag) {
        return;
      }
    }

    final Injector taskletInjector = taskletBaseInjector.forkInjector(taskletConf);
    final Tasklet tasklet = taskletInjector.getInstance(Tasklet.class);
    final TaskletMsgHandler taskletMsgHandler = taskletInjector.getInstance(TaskletMsgHandler.class);

    LOG.log(Level.INFO, "Send tasklet start res msg. tasklet Id: {0}", taskletId);
    msgSender.sendTaskletStatusMsg(taskletId, TaskletStatusType.Running);
    taskletMap.put(taskletId, Pair.of(tasklet, taskletMsgHandler));

    executorService.submit(() -> {
      boolean isSuccess;
      try {
        LOG.log(Level.INFO, "Run tasklet. Id: {0}", taskletId);
        tasklet.run();
        isSuccess = true;
        LOG.log(Level.SEVERE, "Tasklet done. Id: " + taskletId);
      } catch (Exception e) {
        isSuccess = false;
        LOG.log(Level.SEVERE, "Tasklet fail. Id: " + taskletId, e);
      }

      taskletMap.remove(taskletId);

      final TaskletStatusType statusType = isSuccess ? TaskletStatusType.Done : TaskletStatusType.Failed;
      msgSender.sendTaskletStatusMsg(taskletId, statusType);
    });
  }

  public void stopTasklet(final String taskletId) {
    taskletMap.get(taskletId).getLeft().close();
  }

  public synchronized void close() {
    closeFlag = true;

    // stop all tasklets
    taskletMap.values().forEach((taskletPair -> taskletPair.getLeft().close()));
  }

  public void onTaskletMsg(final String taskletId, final byte[] message) {
    final TaskletMsgHandler taskletMsgHandler = taskletMap.get(taskletId).getRight();
    taskletMsgHandler.onNext(message);
  }
}
