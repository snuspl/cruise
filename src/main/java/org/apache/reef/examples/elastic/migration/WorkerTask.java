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

import org.apache.reef.elastic.memory.utils.NSWrapper;
import org.apache.reef.elastic.memory.utils.NamedParameters;
import org.apache.reef.elastic.memory.utils.WorkerTaskOptions;
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

/**
 * Worker Task for the example
 */
public final class WorkerTask implements Task {

  private final CommunicationGroupClient communicationGroupClient;
  private final Broadcast.Receiver broadcastReceiver;
  private final NetworkService networkService;
  private final int indexOfWord;
  private final Set<String> destinations;
  private final IdentifierFactory idFactory;
  private int receiveCount;
  private final int numIter = 5;

  public class ReceiveHandler implements EventHandler<Message<String>> {
    @Override
    public void onNext(Message<String> msg) {
      System.out.println("Received from NetworkServiceWrapper");
      System.out.println(msg.getData().toString() + receiveCount);
      receiveCount++;
    }
  }

  @Inject
  public WorkerTask(
      @Parameter(WorkerTaskOptions.IndexOfWord.class) int indexOfWord,
      @Parameter(WorkerTaskOptions.Destinations.class) final Set<String> destinations,
      final GroupCommClient groupCommClient,
      final NSWrapper nsWrapper) {
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(NamedParameters.GroupCommName.class);
    this.broadcastReceiver = communicationGroupClient.getBroadcastReceiver(NamedParameters.Broadcaster.class);
    this.indexOfWord = indexOfWord;
    this.networkService = nsWrapper.getNetworkService();
    this.destinations = destinations;
    this.idFactory = new StringIdentifierFactory();
    // Register a receive handler within WorkerTask into static NetworkServiceHandler
    // You can modify WorkerTask's state by doing it
    nsWrapper.getNetworkServiceHandler().registerReceiverHandler(new ReceiveHandler());
    this.receiveCount = 0;
  }

  public byte[] call(byte[] memento) throws InterruptedException, NetworkException {
    String receivedString = (String) broadcastReceiver.receive();
    System.out.println("Received from GroupComm");
    System.out.println(receivedString.split(" ")[indexOfWord] + "\n");
    System.out.println("Sending via NetworkServiceWrapper");
    for (String destination : destinations) {
      Connection<String> conn = networkService.newConnection(idFactory.getNewInstance(destination));
      try {
        conn.open();
        for (int i = 0; i < numIter; i++) {
          conn.write(receivedString.split(" ")[indexOfWord]);
        }
      } catch (NetworkException e) {
        e.printStackTrace();
      }
      conn.close();
    }
    return null;
  }
}