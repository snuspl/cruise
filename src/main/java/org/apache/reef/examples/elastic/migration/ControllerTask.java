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

package org.apache.reef.examples.elastic.migration;

import org.apache.reef.elastic.memory.utils.NamedParameters;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.group.api.operators.Broadcast;
import org.apache.reef.io.network.group.api.task.CommunicationGroupClient;
import org.apache.reef.io.network.group.api.task.GroupCommClient;
import org.apache.reef.task.Task;

import javax.inject.Inject;

/**
 * Controller Task for the example
 */
public final class ControllerTask implements Task {

  private final CommunicationGroupClient communicationGroupClient;
  private final Broadcast.Sender<String> broadcastSender;

  @Inject
  ControllerTask(final GroupCommClient groupCommClient) {
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(NamedParameters.GroupCommName.class);
    this.broadcastSender = communicationGroupClient.getBroadcastSender(NamedParameters.Broadcaster.class);
  }

  public final byte[] call(byte[] memento) throws InterruptedException, NetworkException {
    String sendingString = "Hello REEF!";
    broadcastSender.send(sendingString);
    return null;
  }
}
