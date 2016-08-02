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

import edu.snu.cay.services.ps.common.resolver.DynamicServerResolver;
import org.apache.reef.task.events.TaskStart;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.Executors;

/**
 * Sends a msg to register itself to start subscribing updates of the EM's routing table in PS servers.
 * {@link edu.snu.cay.services.ps.common.resolver.DynamicServerResolver} will receive the response
 * of the whole routing table and keep being updated when there's any change.
 */
public final class TaskStartHandler implements EventHandler<TaskStart> {
  private final DynamicServerResolver serverResolver;

  @Inject
  private TaskStartHandler(final DynamicServerResolver serverResolver) {
    this.serverResolver = serverResolver;
  }

  @Override
  public void onNext(final TaskStart taskStart) {
    // do initialization asynchronously
    Executors.newSingleThreadExecutor().execute(serverResolver::triggerInitialization);
  }
}
