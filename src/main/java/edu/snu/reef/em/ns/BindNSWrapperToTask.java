/**
 * Copyright (C) 2014 Seoul National University
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

package edu.snu.reef.em.ns;

import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.events.TaskStart;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * Binds NSWrapper to a Task
 */
public final class BindNSWrapperToTask implements EventHandler<TaskStart> {

  private final NetworkService<?> ns;
  private final IdentifierFactory idFac;

  @Inject
  private BindNSWrapperToTask(
      final NSWrapperClient nsWrapperClient,
      @Parameter(NSWrapperParameters.NetworkServiceIdentifierFactory.class) final IdentifierFactory idFac) {
    this.ns = nsWrapperClient.getNetworkService();
    this.idFac = idFac;
  }

  @Override
  public void onNext(final TaskStart task) {
    this.ns.registerId(this.idFac.getNewInstance(task.getId()));
  }
}
