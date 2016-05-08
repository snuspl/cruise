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
package edu.snu.cay.services.em.evaluator.impl.range;

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.common.parameters.KeyCodecName;
import edu.snu.cay.services.em.evaluator.api.DataOperation;
import edu.snu.cay.services.em.evaluator.api.RemoteAccessibleMemoryStore;
import edu.snu.cay.services.em.evaluator.impl.OperationRouter;
import edu.snu.cay.services.em.evaluator.impl.Remove;
import edu.snu.cay.services.em.evaluator.impl.Update;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.utils.LongRangeUtils;
import edu.snu.cay.utils.trace.HTraceUtils;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.network.util.Pair;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Evaluator-side message handler.
 * Processes control message from the driver and data message from
 * other evaluators.
 */
// TODO #451: Support range operation other than long type
@EvaluatorSide
@Private
public final class ElasticMemoryMsgHandler<K> implements EventHandler<Message<AvroElasticMemoryMessage>> {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMsgHandler.class.getName());

  private static final String ON_DATA_MSG = "onDataMsg";
  private static final String ON_CTRL_MSG = "onCtrlMsg";
  private static final String ON_UPDATE_MSG = "onUpdateMsg";
  private static final String ON_OWNERSHIP_MSG = "onOwnershipMsg";

  private final RemoteAccessibleMemoryStore<K> memoryStore;
  private final OperationRouter<K> router;
  private final RemoteOpHandler<K> remoteOpHandler;
  private final Codec<K> keyCodec;
  private final Serializer serializer;
  private final InjectionFuture<ElasticMemoryMsgSender> sender;

  /**
   * Keeps the information for applying the updates after the data is transferred.
   * This allows the data to be accessible while the migration is going on.
   */
  private final ConcurrentMap<String, Update> pendingUpdates = new ConcurrentHashMap<>();

  /**
   * Keeps the moving ranges, so we don't include the units to the migration if they are already moving.
   */
  private final Set<LongRange> movingRanges = Collections.synchronizedSet(new HashSet<LongRange>());

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
    case RoutingTableInitMsg:
      onRoutingTableInitMsg(innerMsg);
      break;

    case RoutingTableUpdateMsg:
      onRoutingTableUpdateMsg(innerMsg);
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

    case UpdateMsg:
      onUpdateMsg(innerMsg);
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
    router.initialize(msg.getDestId().toString(), msg.getRoutingTableInitMsg().getBlockLocations());
  }

  private void onRoutingTableUpdateMsg(final AvroElasticMemoryMessage msg) {
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

  private void onOwnershipMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onOwnershipMsgScope = Trace.startSpan(ON_OWNERSHIP_MSG,
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final String operationId = msg.getOperationId().toString();
      final OwnershipMsg ownershipMsg = msg.getOwnershipMsg();
      final String dataType = ownershipMsg.getDataType().toString();
      final int blockId = ownershipMsg.getBlockId();
      final int oldOwnerId = ownershipMsg.getOldOwnerId();
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
    final List<KeyRange> avroKeyRangeList = (List<KeyRange>) remoteOpMsg.getDataKeys();
    final List<KeyValuePair> dataKVPairList = (List<KeyValuePair>) remoteOpMsg.getDataValues();
    final String operationId = msg.getOperationId().toString();

    // decode data keys
    final List<Pair<K, K>> dataKeyRanges = new ArrayList<>(avroKeyRangeList.size());
    for (final KeyRange keyRange : avroKeyRangeList) {
      final K minKey = keyCodec.decode(keyRange.getMin().array());
      final K maxKey = keyCodec.decode(keyRange.getMax().array());
      dataKeyRanges.add(new Pair<>(minKey, maxKey));
    }

    // decode data values
    final Optional<NavigableMap<K, Object>> dataKeyValueMap;
    if (operationType.equals(DataOpType.PUT)) {
      final NavigableMap<K, Object> dataMap = new TreeMap<>();
      dataKeyValueMap = Optional.of(dataMap);

      final Codec dataCodec = serializer.getCodec(dataType);
      for (final KeyValuePair dataKVPair : dataKVPairList) {
        final K dataKey = keyCodec.decode(dataKVPair.getKey().array());
        final Object dataValue = dataCodec.decode(dataKVPair.getValue().array());
        dataMap.put(dataKey, dataValue);
      }
    } else {
      dataKeyValueMap = Optional.empty();
    }

    final DataOperation operation = new RangeOperationImpl<>(Optional.of(origEvalId),
        operationId, operationType, dataType, dataKeyRanges, dataKeyValueMap);

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
      case IdRange:
        onCtrlMsgIdRange(msg, onCtrlMsgScope);
        break;

      case NumUnits:
        onCtrlMsgNumUnits(msg, onCtrlMsgScope);
        break;

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
   * Creates a data message using the id ranges specified in the given control message and send it.
   */
  private void onCtrlMsgIdRange(final AvroElasticMemoryMessage msg,
                                final TraceScope parentTraceInfo) {
    final String operationId = msg.getOperationId().toString();
    final CtrlMsg ctrlMsg = msg.getCtrlMsg();
    final String dataType = ctrlMsg.getDataType().toString();
    final Codec codec = serializer.getCodec(dataType);

    // pack the extracted items into a single list for message transmission
    // the identifiers for each item are included with the item itself as an UnitIdPair
    final List<UnitIdPair> unitIdPairList = new ArrayList<>();

    // keep the ids of the items to be deleted later
    final SortedSet<Long> ids = new TreeSet<>();

    // extract all data items from my memory store that correspond to
    // the control message's id specification
    // TODO #15: this loop may be creating a gigantic message, and may cause memory problems
    for (final AvroLongRange avroLongRange : ctrlMsg.getIdRange()) {
      final Map<K, Object> idObjectMap =
          memoryStore.getRange(dataType, (K) avroLongRange.getMin(), (K) avroLongRange.getMax());
      for (final Map.Entry<K, Object> idObject : idObjectMap.entrySet()) {
        final long id = (Long) idObject.getKey();
        if (!isMoving(id)) {
          // Include the units only if they are not moving already.
          final UnitIdPair unitIdPair = UnitIdPair.newBuilder()
              .setUnit(ByteBuffer.wrap(codec.encode(idObject.getValue())))
              .setId(id)
              .build();
          unitIdPairList.add(unitIdPair);
          ids.add(id);
        }
      }
    }

    // If there is no data to move, send failure message instead of sending an empty DataMsg.
    if (ids.size() == 0) {
      final String myId = msg.getDestId().toString();
      final String reason = new StringBuilder()
          .append("No data is movable in ").append(myId)
          .append(" of type ").append(dataType)
          .append(". Requested ranges: ").append(Arrays.toString(ctrlMsg.getIdRange().toArray()))
          .toString();
      sender.get().sendFailureMsg(operationId, reason, TraceInfo.fromSpan(parentTraceInfo.getSpan()));
      return;
    }

    final Set<LongRange> ranges = LongRangeUtils.generateDenseLongRanges(ids);

    // The items of the ids will be removed after the migration succeeds.
    pendingUpdates.put(operationId, new Remove(dataType, ranges));

    // Keep the moving ranges to avoid duplicate request, before the data is removed from the MemoryStore.
    movingRanges.addAll(ranges);

    // Block id is undefined, so mark as -1; we will not use this type of message afterward.
    sender.get().sendDataMsg(msg.getDestId().toString(), ctrlMsg.getDataType().toString(), unitIdPairList,
        -1, operationId, TraceInfo.fromSpan(parentTraceInfo.getSpan()));
  }

  /**
   * Creates a data message using the number of units specified in the given control message and send it.
   * This method simply picks the first n entries that are returned by Map.entrySet().iterator().
   */
  private void onCtrlMsgNumUnits(final AvroElasticMemoryMessage msg,
                                 final TraceScope parentTraceInfo) {
    final String operationId = msg.getOperationId().toString();
    final CtrlMsg ctrlMsg = msg.getCtrlMsg();
    final String dataType = ctrlMsg.getDataType().toString();
    final Codec codec = serializer.getCodec(dataType);
    final int numUnits = ctrlMsg.getNumUnits();

    // fetch all items of the given data type from my memory store
    // TODO #15: this clones the entire map of the given data type, and may cause memory problems
    final Map<K, Object> dataMap = memoryStore.getAll(dataType);
    final List<UnitIdPair> unitIdPairList = new ArrayList<>(Math.min(numUnits, dataMap.size()));

    // keep the ids of the items to be deleted later
    final SortedSet<Long> ids = new TreeSet<>();

    // TODO #15: this loop may be creating a gigantic message, and may cause memory problems
    for (final Map.Entry<K, Object> entry : dataMap.entrySet()) {
      if (unitIdPairList.size() >= numUnits) {
        break;
      }

      final Long id = (Long) entry.getKey();
      if (!isMoving(id)) {
        // Include the units only if they are not moving already.
        final UnitIdPair unitIdPair = UnitIdPair.newBuilder()
            .setUnit(ByteBuffer.wrap(codec.encode(entry.getValue())))
            .setId(id)
            .build();
        unitIdPairList.add(unitIdPair);
        ids.add(id);
      }
    }

    // If there is no data to move, send failure message instead of sending an empty DataMsg.
    if (ids.size() == 0) {
      final String myId = msg.getDestId().toString();
      final String reason = new StringBuilder()
          .append("No data is movable in ").append(myId)
          .append(" of type ").append(dataType)
          .append(". Requested numUnits: ").append(numUnits)
          .toString();
      sender.get().sendFailureMsg(operationId, reason, TraceInfo.fromSpan(parentTraceInfo.getSpan()));
      return;
    }

    final Set<LongRange> ranges = LongRangeUtils.generateDenseLongRanges(ids);

    // The items of the ids will be removed after the migration succeeds.
    pendingUpdates.put(operationId, new Remove(dataType, ranges));

    // Keep the moving ranges to avoid duplicate request, before the data is removed from the MemoryStore.
    movingRanges.addAll(ranges);

    // Block id is undefined, so mark as -1; we will not use this type of message afterward.
    sender.get().sendDataMsg(msg.getDestId().toString(), ctrlMsg.getDataType().toString(), unitIdPairList,
        -1, operationId, TraceInfo.fromSpan(parentTraceInfo.getSpan()));
  }

  /**
   * Applies the pending updates (add, remove) to the MemoryStore,
   * and sends the ACK message to the Driver.
   */
  private void onUpdateMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onUpdateMsgScope =
             Trace.startSpan(ON_UPDATE_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final String operationId = msg.getOperationId().toString();
      final UpdateResult updateResult;

      final Update update = pendingUpdates.remove(operationId);
      if (update == null) {
        LOG.log(Level.WARNING, "The update with id {0} seems already handled.", operationId);
        return;
      }

      switch (update.getType()) {

      case ADD:
        // The receiver adds the data into its MemoryStore.
        update.apply(memoryStore);
        updateResult = UpdateResult.RECEIVER_UPDATED;
        break;

      case REMOVE:
        // The sender removes the data from its MemoryStore.
        update.apply(memoryStore);
        movingRanges.removeAll(update.getRanges());
        updateResult = UpdateResult.SENDER_UPDATED;
        break;
      default:
        throw new RuntimeException("Undefined Message type of Update: " + update);
      }

      sender.get().sendUpdateAckMsg(operationId, updateResult, TraceInfo.fromSpan(onUpdateMsgScope.getSpan()));
    }
  }

  /**
   * Checks whether the unit is moving in order to avoid the duplicate request for the ranges that are moving already.
   * @param id Identifier of unit to check.
   * @return {@code true} if the {@code id} is inside a range that is moving.
   */
  private boolean isMoving(final long id) {
    // We need to synchronize manually for using SynchronizedSet.iterator().
    synchronized (movingRanges) {
      final Iterator<LongRange> iterator = movingRanges.iterator();
      while (iterator.hasNext()) {
        final LongRange range = iterator.next();
        if (range.containsLong(id)) {
          return true;
        }
      }
    }
    return false;
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
