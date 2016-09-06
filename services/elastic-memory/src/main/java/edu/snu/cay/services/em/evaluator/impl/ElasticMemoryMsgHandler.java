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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.common.parameters.KeyCodecName;
import edu.snu.cay.services.em.evaluator.api.RemoteAccessibleMemoryStore;
import edu.snu.cay.services.em.evaluator.api.RemoteOpHandler;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.utils.trace.HTraceUtils;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Evaluator-side message handler.
 * Processes control message from the driver and data message from
 * other evaluators.
 */
@EvaluatorSide
@Private
public final class ElasticMemoryMsgHandler<K> implements EventHandler<Message<AvroElasticMemoryMessage>> {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMsgHandler.class.getName());

  private static final String ON_DATA_MSG = "on_data_msg";
  private static final String ON_CTRL_MSG = "on_ctrl_msg";
  private static final String ON_OWNERSHIP_MSG = "on_ownership_msg";
  private static final String ON_TABLE_UPDATE_MSG = "on_table_update_msg";

  private final RemoteAccessibleMemoryStore<K> memoryStore;
  private final OperationRouter<K> router;
  private final RemoteOpHandler remoteOpHandler;
  private final Codec<K> keyCodec;
  private final Serializer serializer;
  private final InjectionFuture<ElasticMemoryMsgSender> sender;

  @Inject
  private ElasticMemoryMsgHandler(final RemoteAccessibleMemoryStore<K> memoryStore,
                                  final OperationRouter<K> router,
                                  final RemoteOpHandler remoteOpHandler,
                                  final InjectionFuture<ElasticMemoryMsgSender> sender,
                                  @Parameter(KeyCodecName.class) final Codec<K> keyCodec,
                                  final Serializer serializer) {
    this.memoryStore = memoryStore;
    this.router = router;
    this.remoteOpHandler = remoteOpHandler;
    this.keyCodec = keyCodec;
    this.serializer = serializer;
    this.sender = sender;
  }

  @Override
  public void onNext(final Message<AvroElasticMemoryMessage> msg) {
    LOG.entering(ElasticMemoryMsgHandler.class.getSimpleName(), "onNext", msg);

    final AvroElasticMemoryMessage innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case RoutingTableInitMsg:
      onRoutingTableInitMsg(innerMsg);
      break;

    case RoutingTableUpdateMsg:
      onRoutingTableUpdateMsg(innerMsg);
      break;

    case RemoteOpReqMsg:
    case RemoteOpResultMsg:
      onRemoteOpMsg(innerMsg);
      break;

    case DataMsg:
      onDataMsg(innerMsg);
      break;

    case CtrlMsg:
      onCtrlMsg(innerMsg);
      break;

    case OwnershipMsg:
      onOwnershipMsg(innerMsg);
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }

    LOG.exiting(ElasticMemoryMsgHandler.class.getSimpleName(), "onNext", msg);
  }

  private void onRoutingTableInitMsg(final AvroElasticMemoryMessage msg) {
    router.initRoutingTableWithDriver(msg.getRoutingTableInitMsg().getBlockLocations());
  }

  private void onRoutingTableUpdateMsg(final AvroElasticMemoryMessage msg) {
    Trace.setProcessId("eval");
    try (final TraceScope onRoutingTableUpdateMsgScope = Trace.startSpan(ON_TABLE_UPDATE_MSG,
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final RoutingTableUpdateMsg routingTableUpdateMsg = msg.getRoutingTableUpdateMsg();

      final List<Integer> blockIds = routingTableUpdateMsg.getBlockIds();
      final int newOwnerId = getStoreId(routingTableUpdateMsg.getNewEvalId().toString());
      final int oldOwnerId = getStoreId(routingTableUpdateMsg.getOldEvalId().toString());

      LOG.log(Level.INFO, "Update routing table. [newOwner: {0}, oldOwner: {1}, blocks: {2}]",
          new Object[]{newOwnerId, oldOwnerId, blockIds});

      for (final int blockId : blockIds) {
        router.updateOwnership(blockId, oldOwnerId, newOwnerId);
      }
    }
  }

  private void onOwnershipMsg(final AvroElasticMemoryMessage msg) {
    Trace.setProcessId("src_eval");
    try (final TraceScope onOwnershipMsgScope = Trace.startSpan("[4]" + ON_OWNERSHIP_MSG,
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final String operationId = msg.getOperationId().toString();
      final OwnershipMsg ownershipMsg = msg.getOwnershipMsg();
      final int blockId = ownershipMsg.getBlockId();
      final int oldOwnerId = ownershipMsg.getOldOwnerId();
      final int newOwnerId = ownershipMsg.getNewOwnerId();

      // Update the owner of the block to the new one.
      // Operations being executed keep a read lock on router while being executed.
      memoryStore.updateOwnership(blockId, oldOwnerId, newOwnerId);

      // After the ownership is updated, the data is never accessed locally,
      // so it is safe to remove the local data block.
      memoryStore.removeBlock(blockId);

      sender.get().sendOwnershipAckMsg(operationId, blockId, TraceInfo.fromSpan(onOwnershipMsgScope.getSpan()));
    }
  }

  /**
   * Passes the request and result msgs of remote op to {@link RemoteOpHandler}.
   */
  private void onRemoteOpMsg(final AvroElasticMemoryMessage msg) {
    remoteOpHandler.onNext(msg);
  }

  /**
   * Puts the data message contents into own memory store.
   */
  private void onDataMsg(final AvroElasticMemoryMessage msg) {
    Trace.setProcessId("dst_eval");
    try (final TraceScope onDataMsgScope = Trace.startSpan("[2]" + ON_DATA_MSG,
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final DataMsg dataMsg = msg.getDataMsg();
      final Codec codec = serializer.getCodec();
      final String operationId = msg.getOperationId().toString();
      final int blockId = dataMsg.getBlockId();

      final Map<K, Object> dataMap = toDataMap(dataMsg.getKeyValuePairs(), codec);
      memoryStore.putBlock(blockId, dataMap);

      // MemoryStoreId is the suffix of context id (Please refer to PartitionManager.registerEvaluator()
      // and ElasticMemoryConfiguration.getServiceConfigurationWithoutNameResolver).
      final int newOwnerId = getStoreId(msg.getDestId().toString());
      final int oldOwnerId = getStoreId(msg.getSrcId().toString());
      memoryStore.updateOwnership(blockId, oldOwnerId, newOwnerId);

      // Notify the driver that the ownership has been updated by setting empty destination id.
      sender.get().sendOwnershipMsg(Optional.empty(), operationId, blockId, oldOwnerId, newOwnerId,
          TraceInfo.fromSpan(onDataMsgScope.getSpan()));
    }
  }

  /**
   * Converts evaluator id to store id.
   */
  private int getStoreId(final String evalId) {
    return Integer.valueOf(evalId.split("-")[1]);
  }

  /**
   * Creates a data message using the control message contents, and then
   * sends the data message to the correct evaluator.
   */
  private void onCtrlMsg(final AvroElasticMemoryMessage msg) {
    Trace.setProcessId("src_eval");
    try (final TraceScope onCtrlMsgScope = Trace.startSpan("[1]" + ON_CTRL_MSG,
      HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final String operationId = msg.getOperationId().toString();
      final String destId = msg.getDestId().toString();

      final CtrlMsg ctrlMsg = msg.getCtrlMsg();
      final List<Integer> blockIds = ctrlMsg.getBlockIds();

      final Codec codec = serializer.getCodec();

      // Send the data as unit of block
      for (final int blockId : blockIds) {
        final Map<K, Object> blockData = memoryStore.getBlock(blockId);
        final List<KeyValuePair> keyValuePairs = toKeyValuePairs(blockData, codec);

        sender.get().sendDataMsg(destId, keyValuePairs, blockId, operationId,
            TraceInfo.fromSpan(onCtrlMsgScope.getSpan()));
      }
    }
  }

  private <V> List<KeyValuePair> toKeyValuePairs(final Map<K, V> blockData,
                                                 final Codec<V> valueCodec) {
    final List<KeyValuePair> kvPairs = new ArrayList<>(blockData.size());
    for (final Map.Entry<K, V> entry : blockData.entrySet()) {
      kvPairs.add(KeyValuePair.newBuilder()
          .setKey(ByteBuffer.wrap(keyCodec.encode(entry.getKey())))
          .setValue(ByteBuffer.wrap(valueCodec.encode(entry.getValue())))
          .build());
    }
    return kvPairs;
  }

  private <V> Map<K, V> toDataMap(final List<KeyValuePair> keyValuePairs,
                                  final Codec<V> valueCodec) {
    final Map<K, V> dataMap = new HashMap<>(keyValuePairs.size());
    for (final KeyValuePair kvPair : keyValuePairs) {
      dataMap.put(
          keyCodec.decode(kvPair.getKey().array()),
          valueCodec.decode(kvPair.getValue().array()));
    }
    return dataMap;
  }
}
