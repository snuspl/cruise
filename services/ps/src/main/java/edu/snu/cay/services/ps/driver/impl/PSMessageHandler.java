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

import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.driver.api.RoutingInfo;
import edu.snu.cay.services.ps.avro.AvroParameterServerMsg;
import edu.snu.cay.services.ps.avro.RoutingTableRespMsg;
import edu.snu.cay.services.ps.avro.Type;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Receives the messages from ParameterServers and ParameterWorkers.
 * Currently used to exchange the routing table between
 * {@link edu.snu.cay.services.ps.server.partitioned.DynamicPartitionedParameterServer} and
 * {@link edu.snu.cay.services.ps.common.partitioned.resolver.DynamicServerResolver}.
 */
@DriverSide
public final class PSMessageHandler implements EventHandler<Message<AvroParameterServerMsg>> {
  private static final Logger LOG = Logger.getLogger(PSMessageHandler.class.getName());
  private final ElasticMemory elasticMemory;
  private final InjectionFuture<PSMessageSender> sender;

  @Inject
  private PSMessageHandler(final InjectionFuture<PSMessageSender> sender,
                           final ElasticMemory elasticMemory) {
    this.elasticMemory = elasticMemory;
    this.sender = sender;
  }

  @Override
  public void onNext(final Message<AvroParameterServerMsg> avroParameterServerMsgMessage) {
    final String srcId = avroParameterServerMsgMessage.getSrcId().toString();

    final AvroParameterServerMsg msg = SingleMessageExtractor.extract(avroParameterServerMsgMessage);
    if (msg.getType() == Type.RoutingTableReqMsg) {
      final RoutingInfo routingInfo = elasticMemory.getRoutingInfo();

      final List<Integer> blockIds = new ArrayList<>(routingInfo.getBlockIdToStoreId().size());
      final List<Integer> storeIds = new ArrayList<>(routingInfo.getBlockIdToStoreId().size());

      for (final Map.Entry<Integer, Integer> entry : routingInfo.getBlockIdToStoreId().entrySet()) {
        blockIds.add(entry.getKey());
        storeIds.add(entry.getValue());
      }

      final RoutingTableRespMsg routingTableRespMsg = RoutingTableRespMsg.newBuilder()
          .setBlockIds(blockIds)
          .setMemoryStoreIds(storeIds)
          .setBlockSize(routingInfo.getBlockSize())
          .build();

      final AvroParameterServerMsg responseMsg =
          AvroParameterServerMsg.newBuilder()
              .setType(Type.RoutingTableReplyMsg)
              .setRoutingTableRespMsg(routingTableRespMsg).build();

      sender.get().send(srcId, responseMsg);
    } else {
      LOG.log(Level.SEVERE, "Received {0}. Ignore it.", msg);
    }
  }
}
