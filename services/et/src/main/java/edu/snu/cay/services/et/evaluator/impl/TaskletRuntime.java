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

import edu.snu.cay.services.et.evaluator.api.MessageSender;
import edu.snu.cay.services.et.evaluator.api.Tasklet;
import edu.snu.cay.utils.CatchableExecutors;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
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

  private final ExecutorService executorService = CatchableExecutors.newFixedThreadPool(NUM_TASKLETS);

  @Inject
  private TaskletRuntime(final Injector taskletBaseInjector,
                         final MessageSender msgSender) {
    this.taskletBaseInjector = taskletBaseInjector;
    this.msgSender = msgSender;
  }

  public void startTasklet(final String taskletId, final Configuration taskletConf) throws InjectionException {
    if (closeFlag.get()) {
      return;
    }

    final Injector taskletInjector = taskletBaseInjector.forkInjector(taskletConf);
    final Tasklet tasklet = taskletInjector.getInstance(Tasklet.class);

    LOG.log(Level.INFO, "Send tasklet start res msg. tasklet Id: {0}", taskletId);
    msgSender.sendTaskletStartResMsg(taskletId);

    executorService.submit(() -> {
      boolean isSuccess;
      try {
        LOG.log(Level.INFO, "Run tasklet. Id: {0}", taskletId);
        tasklet.run();
        isSuccess = true;
      } catch (Exception e) {
        isSuccess = false;
        LOG.log(Level.SEVERE, "Task fail", e);
      }
      LOG.log(Level.INFO, "Tasklet run finish. IsSuccess: {0}", isSuccess);
      msgSender.sendTaskletStopResMsg(taskletId, isSuccess);
    });
  }

  public void stopTasklet(final String taskletId) {

  }

  private final AtomicBoolean closeFlag = new AtomicBoolean(false);

  public void close() {
    closeFlag.set(true);
    // stop all tasklets
  }
}
