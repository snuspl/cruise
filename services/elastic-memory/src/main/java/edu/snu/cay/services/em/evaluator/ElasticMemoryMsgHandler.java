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
import org.apache.htrace.Trace;
import org.apache.htrace.TraceInfo;
import org.apache.htrace.TraceScope;
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
      for (final UnitIdPair unitIdPair : dataMsg.getUnits()) {
        final byte[] data = unitIdPair.getUnit().array();
        final long id = unitIdPair.getId();
        memoryStore.getElasticStore().put(dataType, id, codec.decode(data));
      }

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
            memoryStore.getElasticStore().removeRange(dataType, avroLongRange.getMin(), avroLongRange.getMax());
        numObject += idObjectMap.size();
        idObjectMapSet.add(idObjectMap);
      }

      // pack the extracted items into a single list for message transmission
      // the identifiers for each item are included with the item itself as an UnitIdPair
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
          msg.getOperationId().toString(), TraceInfo.fromSpan(onCtrlMsgScope.getSpan()));
    }
  }
}
