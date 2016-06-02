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
package edu.snu.cay.dolphin.bsp.core.sync;

import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.events.TaskStart;
import org.apache.reef.task.events.TaskStop;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * Register and unregister task ids to and from the NetworkConnectionService.
 */
@Unit
public final class ControllerTaskSyncRegister {

  private final SyncNetworkSetup syncNetworkSetup;
  private final IdentifierFactory identifierFactory;

  @Inject
  private ControllerTaskSyncRegister(final SyncNetworkSetup syncNetworkSetup,
                                     final IdentifierFactory identifierFactory) {
    this.syncNetworkSetup = syncNetworkSetup;
    this.identifierFactory = identifierFactory;
  }

  public final class RegisterTaskHandler implements EventHandler<TaskStart> {
    @Override
    public void onNext(final TaskStart taskStart) {
      syncNetworkSetup.registerConnectionFactory(identifierFactory.getNewInstance(taskStart.getId()));
    }
  }

  public final class UnregisterTaskHandler implements EventHandler<TaskStop> {
    @Override
    public void onNext(final TaskStop taskStop) {
      syncNetworkSetup.unregisterConnectionFactory();
    }
  }
}
