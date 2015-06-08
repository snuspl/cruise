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

package edu.snu.reef.em.examples.elastic.migration;

import edu.snu.reef.elastic.memory.task.ElasticMemoryMessageSender;
import edu.snu.reef.elastic.memory.task.MemoryStoreClient;
import edu.snu.reef.em.examples.parameters.*;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.group.api.operators.Broadcast;
import org.apache.reef.io.network.group.api.task.CommunicationGroupClient;
import org.apache.reef.io.network.group.api.task.GroupCommClient;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.Task;

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

  private final CmpTaskReady cmpTaskReady;
  private final HeartBeatTriggerManager heartBeatTriggerManager;

  @Inject
  public CmpTask(
      @Parameter(WorkerTaskOptions.Destinations.class) final Set<String> destinations,
      final GroupCommClient groupCommClient,
      final ElasticMemoryMessageSender elasticMemoryMessageSender,
      final MemoryStoreClient memoryStoreClient,
      final CmpTaskReady cmpTaskReady,
      final HeartBeatTriggerManager heartBeatTriggerManager) {
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(CommGroupName.class);
    this.broadcastReceiver = communicationGroupClient.getBroadcastReceiver(DataBroadcast.class);
    this.elasticMemoryMessageSender = elasticMemoryMessageSender;
    this.destinations = destinations;
    this.memoryStoreClient = memoryStoreClient;
    this.cmpTaskReady = cmpTaskReady;
    this.heartBeatTriggerManager = heartBeatTriggerManager;
  }

  public byte[] call(byte[] memento) throws InterruptedException, NetworkException {
    LOG.log(Level.INFO, "CmpTask commencing...");
    String receivedString = broadcastReceiver.receive();
    System.out.println("Received via GroupComm: " + receivedString);
    System.out.println();

    System.out.println("Store a string in EM");
    memoryStoreClient.putMovable("String", destinations.toString() + " must not see this.");
    System.out.println(memoryStoreClient.get("String"));
    System.out.println();

    cmpTaskReady.setReady(true);
    heartBeatTriggerManager.triggerHeartBeat();

    System.out.println("Waiting 10 seconds.");
    Thread.sleep(10000);
    System.out.println("Waked up!");

    System.out.println("New data is");
    System.out.println(memoryStoreClient.get("String"));

//    System.out.println("Sending via NetworkServiceWrapper");
//    System.out.println(destinations);
//    for (final String dest : destinations) {
//      elasticMemoryMessageSender.send(dest, "clazzName!", "clazzInstance".getBytes());
//    }

    return null;
  }
}