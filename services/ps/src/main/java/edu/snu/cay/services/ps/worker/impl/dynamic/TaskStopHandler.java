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
package edu.snu.cay.services.ps.worker.impl.dynamic;

import edu.snu.cay.services.ps.worker.impl.WorkerMsgSender;
import org.apache.reef.task.events.TaskStop;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sends a msg to deregister the worker itself to stop subscribing the updates of EM routing table in PS servers.
 */
public final class TaskStopHandler implements EventHandler<TaskStop> {
  private static final Logger LOG = Logger.getLogger(TaskStopHandler.class.getName());
  private WorkerMsgSender sender;

  @Inject
  private TaskStopHandler(final WorkerMsgSender sender) {
    this.sender = sender;
  }

  @Override
  public void onNext(final TaskStop taskStop) {
    LOG.log(Level.FINE, "Task {0} sends a msg to deregister itself to stop subscribing updates in routing table",
        taskStop.getId());
    sender.sendWorkerDeregisterMsg();
  }
}
