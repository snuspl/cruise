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
package edu.snu.cay.services.ps.driver.impl;

import edu.snu.cay.services.ps.avro.AvroPSMsg;
import edu.snu.cay.services.ps.avro.IdMapping;
import edu.snu.cay.services.ps.avro.Type;
import edu.snu.cay.services.ps.avro.WorkerRegisterReplyMsg;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Receives the messages from ParameterServers and ParameterWorkers.
 * Currently used to exchange the routing table between
 * {@link edu.snu.cay.services.ps.server.impl.dynamic.DynamicParameterServer} and
 * {@link edu.snu.cay.services.ps.common.resolver.DynamicServerResolver}.
 */
// TODO #553: Should be instantiated only when dynamic PS is used.
@DriverSide
public final class DriverSideMsgHandler implements EventHandler<Message<AvroPSMsg>> {
  private final EMRoutingTableManager routingTableManager;
  private final InjectionFuture<PSMessageSender> sender;

  @Inject
  private DriverSideMsgHandler(final EMRoutingTableManager routingTableManager,
                               final InjectionFuture<PSMessageSender> sender) {
    this.routingTableManager = routingTableManager;
    this.sender = sender;
  }

  @Override
  public void onNext(final Message<AvroPSMsg> avroParameterServerMsgMessage) {
    final String srcId = avroParameterServerMsgMessage.getSrcId().toString();

    final AvroPSMsg msg = SingleMessageExtractor.extract(avroParameterServerMsgMessage);
    switch (msg.getType()) {
    case WorkerRegisterMsg:
      onWorkerRegisterMsg(srcId);
      break;

    case WorkerDeregisterMsg:
      onWorkerDeregisterMsg(srcId);
      break;

    default:
      throw new RuntimeException("Unexpected message type: " + msg.getType().toString());
    }
  }

  private void onWorkerRegisterMsg(final String srcId) {
    final EMRoutingTable routingTable = routingTableManager.registerWorker(srcId);
    final int numTotalBlocks = routingTable.getNumTotalBlocks();
    final List<IdMapping> idMappings = new ArrayList<>(routingTable.getStoreIdToEndpointId().size());
    final Map<Integer, String> storeIdToEndpointId = routingTable.getStoreIdToEndpointId();
    for (final Map.Entry<Integer, Set<Integer>> entry : routingTable.getStoreIdToBlockIds().entrySet()) {
      final int storeId = entry.getKey();
      final List<Integer> blockIds = new ArrayList<>(entry.getValue());
      final IdMapping idMapping = IdMapping.newBuilder()
          .setMemoryStoreId(storeId)
          .setBlockIds(blockIds)
          .setEndpointId(storeIdToEndpointId.get(storeId))
          .build();
      idMappings.add(idMapping);
    }

    final WorkerRegisterReplyMsg workerRegisterReplyMsg = WorkerRegisterReplyMsg.newBuilder()
        .setIdMappings(idMappings)
        .setNumTotalBlocks(numTotalBlocks)
        .build();

    final AvroPSMsg responseMsg =
        AvroPSMsg.newBuilder()
            .setType(Type.WorkerRegisterReplyMsg)
            .setWorkerRegisterReplyMsg(workerRegisterReplyMsg)
            .build();

    sender.get().send(srcId, responseMsg);
  }

  private void onWorkerDeregisterMsg(final String srcId) {
    routingTableManager.deregisterWorker(srcId);
  }
}
