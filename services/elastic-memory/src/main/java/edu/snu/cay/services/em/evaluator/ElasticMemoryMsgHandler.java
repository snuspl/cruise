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
package edu.snu.cay.services.em.evaluator;

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.services.em.trace.HTraceUtils;
import edu.snu.cay.services.em.utils.SingleMessageExtractor;
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
import java.util.logging.Logger;

/**
 * Evaluator-side message handler.
 * Processes control message from the driver and data message from
 * other evaluators.
 */
@EvaluatorSide
public final class ElasticMemoryMsgHandler implements EventHandler<Message<AvroElasticMemoryMessage>> {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMsgHandler.class.getName());

  private static final String ON_DATA_MSG = "onDataMsg";
  private static final String ON_CTRL_MSG = "onCtrlMsg";
  private static final String ON_UPDATE_MSG = "onUpdateMsg";

  private final MemoryStore memoryStore;
  private final Serializer serializer;
  private final InjectionFuture<ElasticMemoryMsgSender> sender;

  private final ConcurrentHashMap<CharSequence, UpdateInfo> pendingUpdates = new ConcurrentHashMap<>();

  @Inject
  private ElasticMemoryMsgHandler(final MemoryStore memoryStore,
                                  final InjectionFuture<ElasticMemoryMsgSender> sender,
                                  final Serializer serializer) {
    this.memoryStore = memoryStore;
    this.serializer = serializer;
    this.sender = sender;
  }

  @Override
  public void onNext(final Message<AvroElasticMemoryMessage> msg) {
    LOG.entering(ElasticMemoryMsgHandler.class.getSimpleName(), "onNext", msg);

    final AvroElasticMemoryMessage innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case DataMsg:
      onDataMsg(innerMsg);
      break;

    case CtrlMsg:
      onCtrlMsg(innerMsg);
      break;

    case UpdateMsg:
      onUpdateMsg(innerMsg);
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }

    LOG.exiting(ElasticMemoryMsgHandler.class.getSimpleName(), "onNext", msg);
  }

  /**
   * Applies the pending updates (add, remove) to the MemoryStore.
   */
  private void onUpdateMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onUpdateMsgScope =
             Trace.startSpan(ON_UPDATE_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final CharSequence operationId = msg.getOperationId();
      final UpdateResult updateResult;

      final UpdateInfo updateInfo = pendingUpdates.get(operationId);
      if (updateInfo == null) {
        updateResult = UpdateResult.FAILED;
      } else {
        switch (updateInfo.getType()) {
        case ADD:
          final AddInfo addInfo = (AddInfo) updateInfo;
          applyAdd(addInfo);
          updateResult = UpdateResult.RECEIVER_UPDATED;
          break;
        case REMOVE:
          final RemoveInfo removeInfo = (RemoveInfo) updateInfo;
          applyRemove(removeInfo);
          updateResult = UpdateResult.SENDER_UPDATED;
          break;
        default:
          throw new RuntimeException("Undefined Message type of UpdateInfo: " + updateInfo);
        }
      }

      sendUpdateAckMsg(operationId, updateResult, onUpdateMsgScope);
    }
  }

   /**
   * Adds the data to the MemoryStore.
   */
  private void applyAdd(final AddInfo addInfo) {
    final String dataType = addInfo.getDataType();
    final Codec codec = serializer.getCodec(dataType);

    for (final UnitIdPair unitIdPair : addInfo.getUnitIdPairs()) {
      final byte[] data = unitIdPair.getUnit().array();
      final long id = unitIdPair.getId();
      memoryStore.getElasticStore().put(dataType, id, codec.decode(data));
    }
  }

  /**
   * Removes the data from the MemoryStore. It is guaranteed that the data is transferred,
   * and the Driver updated its partition status successfully.
   */
  private void applyRemove(final RemoveInfo removeInfo) {
    final String dataType = removeInfo.getDataType();
    for (final long id : removeInfo.getIds()) {
      memoryStore.getElasticStore().remove(dataType, id);
    }
  }

  /**
   * Send the ACK message of the update operation.
   */
  private void sendUpdateAckMsg(final CharSequence operationId,
                                final UpdateResult result,
                                final TraceScope traceScope) {
    sender.get().sendUpdateAckMsg(operationId.toString(), result, TraceInfo.fromSpan(traceScope.getSpan()));
  }

  /**
   * Puts the data message contents into own memory store.
   */
  private void onDataMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onDataMsgScope = Trace.startSpan(ON_DATA_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final DataMsg dataMsg = msg.getDataMsg();
      final String dataType = dataMsg.getDataType().toString();

      // store the items in the pendingUpdates, so the items will be added later.
      pendingUpdates.put(msg.getOperationId(), new AddInfo(dataType, dataMsg.getUnits()));

      sender.get().sendResultMsg(true,
          msg.getOperationId().toString(), TraceInfo.fromSpan(onDataMsgScope.getSpan()));
    }
  }

  /**
   * Create a data message using the control message contents, and then
   * send the data message to the correct evaluator.
   */
  private void onCtrlMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onCtrlMsgScope = Trace.startSpan(ON_CTRL_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final CtrlMsg ctrlMsg = msg.getCtrlMsg();
      final String dataType = ctrlMsg.getDataType().toString();
      final Codec codec = serializer.getCodec(dataType);

      // extract all data items from my memory store that correspond to
      // the control message's id specification
      final Set<Map<Long, Object>> idObjectMapSet = new HashSet<>();
      int numObject = 0;
      for (final AvroLongRange avroLongRange : ctrlMsg.getIdRange()) {
        final Map<Long, Object> idObjectMap =
            memoryStore.getElasticStore().getRange(dataType, avroLongRange.getMin(), avroLongRange.getMax());
        numObject += idObjectMap.size();
        idObjectMapSet.add(idObjectMap);
      }

      // collect the identifiers of the extracted items to remove them after the data is sent successfully.
      // for message transmission, the items are packed into a single list of UnitIdPair,
      // each of which includes the identifier of each item
      final List<UnitIdPair> unitIdPairList = new ArrayList<>(numObject);
      final Set<Long> ids = new HashSet<>(numObject);
      for (final Map<Long, Object> idObjectMap : idObjectMapSet) {
        for (final Map.Entry<Long, Object> idObject : idObjectMap.entrySet()) {
          final long id = idObject.getKey();
          ids.add(id);

          final Object unit = idObject.getValue();
          final UnitIdPair unitIdPair = UnitIdPair.newBuilder()
              .setUnit(ByteBuffer.wrap(codec.encode(unit)))
              .setId(id)
              .build();
          unitIdPairList.add(unitIdPair);
        }
      }

      // Register the information of this operation to remove the items from the MemoryStore later.
      pendingUpdates.put(msg.getOperationId(), new RemoveInfo(dataType, ids));

      sender.get().sendDataMsg(msg.getDestId().toString(), ctrlMsg.getDataType().toString(), unitIdPairList,
          msg.getOperationId().toString(), TraceInfo.fromSpan(onCtrlMsgScope.getSpan()));
    }
  }
}
