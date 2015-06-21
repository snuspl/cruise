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
import org.apache.reef.io.network.impl.NetworkServiceParameters;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.events.TaskStop;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * Handler which unbinds NSWrapper from a Task
 */
public final class UnbindNSWrapperFromTask implements EventHandler<TaskStop> {

  private final NetworkService<?> ns;
  private final IdentifierFactory idFac;

  @Inject
  private UnbindNSWrapperFromTask(
      final NSWrapperClient nsWrapperDriver,
      @Parameter(NetworkServiceParameters.NetworkServiceIdentifierFactory.class) final IdentifierFactory idFac) {
    this.ns = nsWrapperDriver.getNetworkService();
    this.idFac = idFac;
  }

  @Override
  public void onNext(final TaskStop task) {
    this.ns.unregisterId(this.idFac.getNewInstance(task.getId()));
  }
}
