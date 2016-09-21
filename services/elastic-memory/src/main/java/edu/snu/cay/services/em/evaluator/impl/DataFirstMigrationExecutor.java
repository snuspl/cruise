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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.common.parameters.KeyCodecName;
import edu.snu.cay.services.em.evaluator.api.MigrationExecutor;
import edu.snu.cay.services.em.evaluator.api.RemoteAccessibleMemoryStore;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.utils.trace.HTraceUtils;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xyzi on 9/19/16.
 */
public final class DataFirstMigrationExecutor<K> implements MigrationExecutor<K> {

  private final RemoteAccessibleMemoryStore<K> memoryStore;
  private final InjectionFuture<ElasticMemoryMsgSender> sender;

  private final Codec<K> keyCodec;
  private final Serializer serializer;

  @Inject
  private DataFirstMigrationExecutor(final RemoteAccessibleMemoryStore<K> memoryStore,
                                     final InjectionFuture<ElasticMemoryMsgSender> sender,
                                     @Parameter(KeyCodecName.class)final Codec<K> keyCodec,
                                     final Serializer serializer) {
    this.memoryStore = memoryStore;
    this.sender = sender;
    this.keyCodec = keyCodec;
    this.serializer = serializer;
  }

  @Override
  public void onNext(final MigrationMsg msg) {
    switch (msg.getType()) {
    case MoveInitMsg:
      onMoveInitMsg(msg);
      break;

    case DataMsg:
      onDataMsg(msg);
      break;

    case OwnershipMsg:
      onOwnershipMsg(msg);
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  /**
   * Initiates move by sending data messages to the destination evaluator.
   */
  private void onMoveInitMsg(final MigrationMsg msg) {
    Trace.setProcessId("src_eval");
    try (final TraceScope onMoveInitMsgScope = Trace.startSpan("on_move_init_msg",
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final String operationId = msg.getOperationId().toString();

      final MoveInitMsg moveInitMsg = msg.getMoveInitMsg();
      final String recvId = moveInitMsg.getRecvEvalId().toString();
      final List<Integer> blockIds = moveInitMsg.getBlockIds();

      final TraceInfo traceInfo = TraceInfo.fromSpan(onMoveInitMsgScope.getSpan());

      // Send the data as unit of block
      for (final int blockId : blockIds) {
        final Map<K, Object> blockData = memoryStore.getBlock(blockId);

        final List<KeyValuePair> keyValuePairs;
        try (final TraceScope encodeDataScope = Trace.startSpan("encode_data", traceInfo)) {
          keyValuePairs = toKeyValuePairs(blockData, serializer.getCodec());
        }

        sender.get().sendDataMsg(recvId, keyValuePairs, blockId, operationId, traceInfo);
      }
    }
  }

  /**
   * Puts the data message contents into own memory store.
   */
  private void onDataMsg(final MigrationMsg msg) {
    final DataMsg dataMsg = msg.getDataMsg();
    final String operationId = msg.getOperationId().toString();
    final int blockId = dataMsg.getBlockId();

    Trace.setProcessId("dst_eval");
    try (final TraceScope onDataMsgScope = Trace.startSpan(String.format("on_data_msg. blockId: %d", blockId),
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final TraceInfo traceInfo = TraceInfo.fromSpan(onDataMsgScope.getSpan());

      final Map<K, Object> dataMap;
      try (final TraceScope decodeDataScope = Trace.startSpan("decode_data", traceInfo)) {
        dataMap = toDataMap(dataMsg.getKeyValuePairs(), serializer.getCodec());
      }

      memoryStore.putBlock(blockId, dataMap);

      final int newOwnerId = getStoreId(dataMsg.getRecvEvalId().toString());
      final int oldOwnerId = getStoreId(dataMsg.getSendEvalId().toString());
      memoryStore.updateOwnership(blockId, oldOwnerId, newOwnerId);

      // Notify the driver that the ownership has been updated by setting empty destination id.
      sender.get().sendOwnershipMsg(Optional.empty(), operationId, blockId, oldOwnerId, newOwnerId, traceInfo);
    }
  }

  private void onOwnershipMsg(final MigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final OwnershipMsg ownershipMsg = msg.getOwnershipMsg();
    final int blockId = ownershipMsg.getBlockId();
    final int oldOwnerId = ownershipMsg.getOldOwnerId();
    final int newOwnerId = ownershipMsg.getNewOwnerId();

    Trace.setProcessId("src_eval");
    try (final TraceScope onOwnershipMsgScope = Trace.startSpan(String.format("on_ownership_msg. blockId: %d", blockId),
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      // Update the owner of the block to the new one.
      // Operations being executed keep a read lock on router while being executed.
      memoryStore.updateOwnership(blockId, oldOwnerId, newOwnerId);

      // After the ownership is updated, the data is never accessed locally,
      // so it is safe to remove the local data block.
      memoryStore.removeBlock(blockId);

      sender.get().sendBlockMovedMsg(operationId, blockId, TraceInfo.fromSpan(onOwnershipMsgScope.getSpan()));
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

  /**
   * Converts evaluator id to store id.
   * TODO #509: remove assumption on the format of context Id
   */
  private int getStoreId(final String evalId) {
    // MemoryStoreId is the suffix of context id (Please refer to PartitionManager.registerEvaluator()
    // and ElasticMemoryConfiguration.getServiceConfigurationWithoutNameResolver()).
    return Integer.valueOf(evalId.split("-")[1]);
  }
}
