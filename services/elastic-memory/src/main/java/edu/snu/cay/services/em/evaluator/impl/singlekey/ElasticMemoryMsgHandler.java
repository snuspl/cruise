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
package edu.snu.cay.services.em.evaluator.impl.singlekey;

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.common.parameters.KeyCodecName;
import edu.snu.cay.services.em.evaluator.api.DataOperation;
import edu.snu.cay.services.em.evaluator.api.RemoteAccessibleMemoryStore;
import edu.snu.cay.services.em.evaluator.impl.OperationRouter;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.utils.SingleMessageExtractor;
import edu.snu.cay.utils.trace.HTraceUtils;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

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

  private static final String ON_DATA_MSG = "onDataMsg";
  private static final String ON_CTRL_MSG = "onCtrlMsg";
  private static final String ON_OWNERSHIP_MSG = "onOwnershipMsg";

  private final RemoteAccessibleMemoryStore<K> memoryStore;
  private final OperationRouter<K> router;
  private final RemoteOpHandler<K> remoteOpHandler;
  private final Codec<K> keyCodec;
  private final Serializer serializer;
  private final InjectionFuture<ElasticMemoryMsgSender> sender;

  @Inject
  private ElasticMemoryMsgHandler(final RemoteAccessibleMemoryStore<K> memoryStore,
                                  final OperationRouter<K> router,
                                  final RemoteOpHandler<K> remoteOpHandler,
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
    case RoutingInitMsg:
      onRoutingInitMsg(innerMsg);
      break;

    case RoutingUpdateMsg:
      onRoutingUpdateMsg(innerMsg);
      break;

    case RemoteOpMsg:
      onRemoteOpMsg(innerMsg);
      break;

    case RemoteOpResultMsg:
      onRemoteOpResultMsg(innerMsg);
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

  private void onRoutingInitMsg(final AvroElasticMemoryMessage msg) {
    router.initialize(msg.getDestId().toString(), msg.getRoutingInitMsg().getBlockLocations());
  }

  private void onRoutingUpdateMsg(final AvroElasticMemoryMessage msg) {
    final RoutingUpdateMsg routingUpdateMsg = msg.getRoutingUpdateMsg();

    final List<Integer> blockIds = routingUpdateMsg.getBlockIds();
    final int newOwnerId = getStoreId(routingUpdateMsg.getNewOwnerId().toString());
    final int oldOwnerId = getStoreId(routingUpdateMsg.getOldOwnerId().toString());

    LOG.log(Level.INFO, "Update routing table. [newOwner: {0}, oldOwner: {1}, blocks: {2}]",
        new Object[]{newOwnerId, oldOwnerId, blockIds});

    for (final int blockId : blockIds) {
      router.updateOwnership(blockId, oldOwnerId, newOwnerId);
    }
  }

  private void onOwnershipMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onOwnershipMsgScope = Trace.startSpan(ON_OWNERSHIP_MSG,
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final String operationId = msg.getOperationId().toString();
      final OwnershipMsg ownershipMsg = msg.getOwnershipMsg();
      final String dataType = ownershipMsg.getDataType().toString();
      final int blockId = ownershipMsg.getBlockId();
      final int oldOwnerId = msg.getOwnershipMsg().getOldOwnerId();
      final int newOwnerId = ownershipMsg.getNewOwnerId();

      // Update the owner of the block to the new one.
      // Operations being executed keep a read lock on router while being executed.
      memoryStore.updateOwnership(dataType, blockId, oldOwnerId, newOwnerId);

      // After the ownership is updated, the data is never accessed locally,
      // so it is safe to remove the local data block.
      memoryStore.removeBlock(dataType, blockId);

      sender.get().sendOwnershipAckMsg(operationId, dataType, blockId,
          TraceInfo.fromSpan(onOwnershipMsgScope.getSpan()));
    }
  }

  /**
   * Handles the data operation sent from the remote memory store.
   */
  private void onRemoteOpMsg(final AvroElasticMemoryMessage msg) {
    final RemoteOpMsg remoteOpMsg = msg.getRemoteOpMsg();
    final String origEvalId = remoteOpMsg.getOrigEvalId().toString();
    final DataOpType operationType = remoteOpMsg.getOpType();
    final String dataType = remoteOpMsg.getDataType().toString();
    final DataKey dataKey = (DataKey) remoteOpMsg.getDataKeys();
    final DataValue dataValue = (DataValue) remoteOpMsg.getDataValues();
    final String operationId = msg.getOperationId().toString();

    // decode data keys
    final K decodedKey = keyCodec.decode(dataKey.getKey().array());

    // decode data values
    final Optional<Object> decodedValue;
    if (operationType.equals(DataOpType.PUT)) {
      final Codec dataCodec = serializer.getCodec(dataType);
      decodedValue = Optional.of(dataCodec.decode(dataValue.getValue().array()));
    } else {
      decodedValue = Optional.empty();
    }

    final DataOperation operation = new SingleKeyOperationImpl<>(Optional.of(origEvalId),
        operationId, operationType, dataType, decodedKey, decodedValue);

    // enqueue operation into memory store
    memoryStore.onNext(operation);
  }

  /**
   * Handles the result of data operation sent from the remote memory store.
   */
  private void onRemoteOpResultMsg(final AvroElasticMemoryMessage msg) {
    remoteOpHandler.onNext(msg);
  }

  /**
   * Puts the data message contents into own memory store.
   */
  private void onDataMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onDataMsgScope = Trace.startSpan(ON_DATA_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final DataMsg dataMsg = msg.getDataMsg();
      final String dataType = dataMsg.getDataType().toString();
      final Codec codec = serializer.getCodec(dataType);
      final String operationId = msg.getOperationId().toString();
      final int blockId = dataMsg.getBlockId();

      final Map<Long, Object> dataMap = toDataMap(dataMsg.getUnits(), codec);
      memoryStore.putBlock(dataType, blockId, (Map<K, Object>) dataMap);

      // MemoryStoreId is the suffix of context id (Please refer to PartitionManager.registerEvaluator()
      // and ElasticMemoryConfiguration.getServiceConfigurationWithoutNameResolver).
      final int newOwnerId = getStoreId(msg.getDestId().toString());
      final int oldOwnerId = getStoreId(msg.getSrcId().toString());
      memoryStore.updateOwnership(dataType, blockId, oldOwnerId, newOwnerId);

      // Notify the driver that the ownership has been updated by setting empty destination id.
      sender.get().sendOwnershipMsg(Optional.<String>empty(), operationId, dataType, blockId, oldOwnerId, newOwnerId,
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
    try (final TraceScope onCtrlMsgScope = Trace.startSpan(ON_CTRL_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final CtrlMsgType ctrlMsgType = msg.getCtrlMsg().getCtrlMsgType();
      switch (ctrlMsgType) {
      case Blocks:
        onCtrlMsgBlocks(msg, onCtrlMsgScope);
        break;

      default:
        throw new RuntimeException("Unexpected control message type: " + ctrlMsgType);
      }
    }
  }

  /**
   * Called when the Driver initiates data migration.
   */
  private void onCtrlMsgBlocks(final AvroElasticMemoryMessage msg, final TraceScope parentTraceInfo) {
    final String operationId = msg.getOperationId().toString();
    final CtrlMsg ctrlMsg = msg.getCtrlMsg();
    final String dataType = ctrlMsg.getDataType().toString();
    final Codec codec = serializer.getCodec(dataType);
    final List<Integer> blockIds = ctrlMsg.getBlockIds();

    // Send the data as unit of block
    for (final int blockId : blockIds) {
      final Map<K, Object> blockData = memoryStore.getBlock(dataType, blockId);
      final List<UnitIdPair> unitIdPairList = toUnitIdPairs((Map<Long, Object>) blockData, codec);
      sender.get().sendDataMsg(msg.getDestId().toString(), ctrlMsg.getDataType().toString(), unitIdPairList,
          blockId, operationId, TraceInfo.fromSpan(parentTraceInfo.getSpan()));
    }
  }

  /**
   * Converts the Avro unit id pairs, to a data map.
   */
  private Map<Long, Object> toDataMap(final List<UnitIdPair> unitIdPairs, final Codec codec) {
    final Map<Long, Object> dataMap = new HashMap<>(unitIdPairs.size());
    for (final UnitIdPair unitIdPair : unitIdPairs) {
      dataMap.put(unitIdPair.getId(), codec.decode(unitIdPair.getUnit().array()));
    }
    return dataMap;
  }

  /**
   * Converts the data map into unit id pairs, which can be transferred via Avro.
   */
  private List<UnitIdPair> toUnitIdPairs(final Map<Long, Object> dataMap, final Codec codec) {
    final List<UnitIdPair> unitIdPairs = new ArrayList<>(dataMap.size());
    for (final Map.Entry<Long, Object> idObject : dataMap.entrySet()) {
      final long id = idObject.getKey();
      // Include the units only if they are not moving already.
      final UnitIdPair unitIdPair = UnitIdPair.newBuilder()
          .setUnit(ByteBuffer.wrap(codec.encode(idObject.getValue())))
          .setId(id)
          .build();
      unitIdPairs.add(unitIdPair);
    }
    return unitIdPairs;
  }
}
