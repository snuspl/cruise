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
package edu.snu.cay.dolphin.async.core.driver.master;

import edu.snu.cay.dolphin.async.core.server.ETServerTask;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * Handles the event to stop the task.
 */
public final class ServerTaskCloseHandler implements EventHandler<CloseEvent> {
  private final ETServerTask task;

  @Inject
  private ServerTaskCloseHandler(final ETServerTask task) {
    this.task = task;
  }

  @Override
  public void onNext(final CloseEvent closeEvent) {
    task.close();
  }
}
