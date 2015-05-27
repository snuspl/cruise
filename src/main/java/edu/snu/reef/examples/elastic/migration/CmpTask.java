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

package edu.snu.reef.examples.elastic.migration;

import edu.snu.reef.elastic.memory.ElasticMemoryMessage;
import edu.snu.reef.elastic.memory.ns.NSWrapper;
import edu.snu.reef.elastic.memory.task.ElasticMemoryMessageSender;
import edu.snu.reef.elastic.memory.task.MemoryStoreClient;
import edu.snu.reef.examples.parameters.DataBroadcast;
import edu.snu.reef.examples.parameters.ExampleKey;
import edu.snu.reef.examples.parameters.WorkerTaskOptions;
import edu.snu.reef.examples.parameters.CommGroupName;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.group.api.operators.Broadcast;
import org.apache.reef.io.network.group.api.task.CommunicationGroupClient;
import org.apache.reef.io.network.group.api.task.GroupCommClient;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Compute Task for the example
 */
public final class CmpTask implements Task {
  private static final Logger LOG = Logger.getLogger(CmpTask.class.getName());
  public static final String TASK_ID_PREFIX = "CmpTask-";

  private final CommunicationGroupClient communicationGroupClient;
  private final Broadcast.Receiver<String> broadcastReceiver;
  private final Set<String> destinations;

  private final ElasticMemoryMessageSender elasticMemoryMessageSender;
  private final MemoryStoreClient memoryStoreClient;

  @Inject
  public CmpTask(
      @Parameter(WorkerTaskOptions.Destinations.class) final Set<String> destinations,
      final GroupCommClient groupCommClient,
      final ElasticMemoryMessageSender elasticMemoryMessageSender,
      final MemoryStoreClient memoryStoreClient) {
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(CommGroupName.class);
    this.broadcastReceiver = communicationGroupClient.getBroadcastReceiver(DataBroadcast.class);
    this.elasticMemoryMessageSender = elasticMemoryMessageSender;
    this.destinations = destinations;
    this.memoryStoreClient = memoryStoreClient;
  }

  public byte[] call(byte[] memento) throws InterruptedException, NetworkException {
    LOG.log(Level.INFO, "CmpTask commencing...");
    String receivedString = broadcastReceiver.receive();
    System.out.println("Received via GroupComm: " + receivedString);
    System.out.println("Store a string in EM");
    memoryStoreClient.putMovable(ExampleKey.class, destinations.toString() + " must not see this.");
    System.out.println(memoryStoreClient.get(ExampleKey.class));
    System.out.println("Sending via NetworkServiceWrapper");
    System.out.println(destinations);
    for (final String dest : destinations) {
      elasticMemoryMessageSender.send(dest, "clazzName!", "clazzInstance".getBytes());
    }
    return null;
  }
}