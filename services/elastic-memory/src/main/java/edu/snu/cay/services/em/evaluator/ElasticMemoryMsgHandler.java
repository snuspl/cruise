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
import edu.snu.cay.utils.LongRangeUtils;
import edu.snu.cay.utils.SingleMessageExtractor;
import edu.snu.cay.utils.avro.AvroLongRange;
import org.apache.commons.lang.math.LongRange;
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

  private final MemoryStore memoryStore;
  private final Serializer serializer;
  private final InjectionFuture<ElasticMemoryMsgSender> sender;

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
      final Codec codec = serializer.getCodec(dataMsg.getDataType().toString());

      // extract data items from the message and store them in my memory store
      final SortedSet<Long> newIds = new TreeSet<>();
      for (final UnitIdPair unitIdPair : dataMsg.getUnits()) {
        final byte[] data = unitIdPair.getUnit().array();
        final long id = unitIdPair.getId();
        newIds.add(id);
        memoryStore.getElasticStore().put(dataType, id, codec.decode(data));
      }

      final Set<LongRange> longRangeSet = LongRangeUtils.generateDenseLongRanges(newIds);
      sender.get().sendResultMsg(true, dataType, longRangeSet,
          msg.getOperationId().toString(), TraceInfo.fromSpan(onDataMsgScope.getSpan()));
    }
  }

  /**
   * Create a data message using the control message contents, and then
   * send the data message to the correct evaluator.
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

      default:
        throw new RuntimeException("Unexpected control message type: " + ctrlMsgType);
      }
    }
  }

  /**
   * Create a data message using the id ranges specified in the given control message and send it.
   */
  private void onCtrlMsgIdRange(final AvroElasticMemoryMessage msg,
                                final TraceScope parentTraceInfo) {
    final CtrlMsg ctrlMsg = msg.getCtrlMsg();
    final String dataType = ctrlMsg.getDataType().toString();
    final Codec codec = serializer.getCodec(dataType);

    // extract all data items from my memory store that correspond to
    // the control message's id specification
    final Set<Map<Long, Object>> idObjectMapSet = new HashSet<>();
    int numObject = 0;
    for (final AvroLongRange avroLongRange : ctrlMsg.getIdRange()) {
      final Map<Long, Object> idObjectMap =
          memoryStore.getElasticStore().removeRange(dataType, avroLongRange.getMin(), avroLongRange.getMax());
      numObject += idObjectMap.size();
      idObjectMapSet.add(idObjectMap);
    }

    // pack the extracted items into a single list for message transmission
    // the identifiers for each item are included with the item itself as an UnitIdPair
    // TODO #90: if this store doesn't contain the expected ids,
    //           then the Driver should be notified (ResultMsg.FAILURE)
    final List<UnitIdPair> unitIdPairList = new ArrayList<>(numObject);
    for (final Map<Long, Object> idObjectMap : idObjectMapSet) {
      for (final Map.Entry<Long, Object> idObject : idObjectMap.entrySet()) {
        final UnitIdPair unitIdPair = UnitIdPair.newBuilder()
            .setUnit(ByteBuffer.wrap(codec.encode(idObject.getValue())))
            .setId(idObject.getKey())
            .build();

        unitIdPairList.add(unitIdPair);
      }
    }

    sender.get().sendDataMsg(msg.getDestId().toString(), ctrlMsg.getDataType().toString(), unitIdPairList,
        msg.getOperationId().toString(), TraceInfo.fromSpan(parentTraceInfo.getSpan()));
  }

  /**
   * Create a data message using the number of units specified in the given control message and send it.
   * This method simply picks the first n entries that are returned by Map.entrySet().iterator().
   */
  private void onCtrlMsgNumUnits(final AvroElasticMemoryMessage msg,
                                 final TraceScope parentTraceInfo) {
    final CtrlMsg ctrlMsg = msg.getCtrlMsg();
    final String dataType = ctrlMsg.getDataType().toString();
    final Codec codec = serializer.getCodec(dataType);
    final int numUnits = ctrlMsg.getNumUnits();

    // fetch all items of the given data type from my memory store
    // TODO #15: this clones the entire map of the given data type, and may cause memory problems
    final Map<Long, Object> dataMap = memoryStore.getElasticStore().getAll(dataType);
    final List<UnitIdPair> unitIdPairList = new ArrayList<>(Math.min(numUnits, dataMap.size()));

    // TODO #15: this loop may be creating a gigantic message, and may cause memory problems
    // TODO #90: if the number of units requested is greater than this memory store's capacity,
    //           then the Driver should be notified (ResultMsg.FAILURE)
    for (final Map.Entry<Long, Object> entry : dataMap.entrySet()) {
      if (unitIdPairList.size() >= numUnits) {
        break;
      }

      final Long key = entry.getKey();
      final Object value = entry.getValue();

      // remove items one by one until the required number of items has been removed
      memoryStore.getElasticStore().remove(dataType, key);

      final UnitIdPair unitIdPair = UnitIdPair.newBuilder()
          .setUnit(ByteBuffer.wrap(codec.encode(value)))
          .setId(key)
          .build();
      unitIdPairList.add(unitIdPair);
    }

    sender.get().sendDataMsg(msg.getDestId().toString(), ctrlMsg.getDataType().toString(), unitIdPairList,
        msg.getOperationId().toString(), TraceInfo.fromSpan(parentTraceInfo.getSpan()));
  }
}
