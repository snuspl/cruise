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
import edu.snu.cay.utils.SingleMessageExtractor;
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

  private final ConcurrentMap<String, Update> pendingUpdates = new ConcurrentHashMap<>();

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
   * Puts the data message contents into own memory store.
   */
  private void onDataMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onDataMsgScope = Trace.startSpan(ON_DATA_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final DataMsg dataMsg = msg.getDataMsg();
      final String dataType = dataMsg.getDataType().toString();
      final Codec codec = serializer.getCodec(dataType);
      final String operationId = msg.getOperationId().toString();

      // store the items in the pendingUpdates, so the items will be added later.
      pendingUpdates.put(operationId, new Add(dataType, codec, dataMsg.getUnits()));

      sender.get().sendResultMsg(true, operationId, TraceInfo.fromSpan(onDataMsgScope.getSpan()));
    }
  }

  /**
   * Create a data message using the control message contents, and then
   * send the data message to the correct evaluator.
   */
  private void onCtrlMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onCtrlMsgScope = Trace.startSpan(ON_CTRL_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final String operationId = msg.getOperationId().toString();
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

      // keep the ids of the items to be deleted later
      final Set<Long> ids = new HashSet<>();

      // pack the extracted items into a single list for message transmission
      // the identifiers for each item are included with the item itself as an UnitIdPair
      // TODO #90: if this store doesn't contain the expected ids,
      //           then the Driver should be notified (ResultMsg.FAILURE)
      // TODO #15: this loop may be creating a gigantic message, and may cause memory problems
      final List<UnitIdPair> unitIdPairList = new ArrayList<>(numObject);
      for (final Map<Long, Object> idObjectMap : idObjectMapSet) {
        for (final Map.Entry<Long, Object> idObject : idObjectMap.entrySet()) {
          final long id = idObject.getKey();
          final UnitIdPair unitIdPair = UnitIdPair.newBuilder()
              .setUnit(ByteBuffer.wrap(codec.encode(idObject.getValue())))
              .setId(id)
              .build();
          unitIdPairList.add(unitIdPair);

          ids.add(id);
        }
      }

      // The items of the ids will be removed after the migration succeeds.
      pendingUpdates.put(operationId, new Remove(dataType, ids));
      sender.get().sendDataMsg(msg.getDestId().toString(), ctrlMsg.getDataType().toString(), unitIdPairList,
          operationId, TraceInfo.fromSpan(onCtrlMsgScope.getSpan()));
    }
  }

  /**
   * Applies the pending updates (add, remove) to the MemoryStore,
   * and send the ACK message to the Driver.
   */
  private void onUpdateMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onUpdateMsgScope =
             Trace.startSpan(ON_UPDATE_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final String operationId = msg.getOperationId().toString();
      final UpdateResult updateResult;

      final Update update = pendingUpdates.remove(operationId);
      if (update == null) {
        sender.get().sendFailureMsg(operationId, "The operation id " + operationId + " does not exist.",
            TraceInfo.fromSpan(onUpdateMsgScope.getSpan()));
        return;
      } else {
        switch (update.getType()) {

        case ADD:
          // ADD is done by the receiver.
          final Add add = (Add) update;
          add.apply(memoryStore);
          updateResult = UpdateResult.RECEIVER_UPDATED;
          break;

        case REMOVE:
          // REMOVE is done by the sender.
          final Remove remove = (Remove) update;
          remove.apply(memoryStore);
          updateResult = UpdateResult.SUCCESS;
          break;
        default:
          throw new RuntimeException("Undefined Message type of Update: " + update);
        }
      }

      sender.get().sendUpdateAckMsg(operationId, updateResult, TraceInfo.fromSpan(onUpdateMsgScope.getSpan()));
    }
  }
}
