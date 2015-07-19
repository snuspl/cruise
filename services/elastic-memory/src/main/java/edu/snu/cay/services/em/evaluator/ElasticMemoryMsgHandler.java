package edu.snu.cay.services.em.evaluator;

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.services.em.trace.HTraceUtils;
import edu.snu.cay.services.em.utils.SingleMessageExtractor;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceInfo;
import org.apache.htrace.TraceScope;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
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
    final TraceScope onDataMsgScope = Trace.startSpan(ON_DATA_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()));
    try {

      final DataMsg dataMsg = msg.getDataMsg();

      final String dataClassName = dataMsg.getDataClassName().toString();
      final Codec codec = serializer.getCodec(dataMsg.getDataClassName().toString());
      for (final UnitIdPair unitIdPair : dataMsg.getUnits()) {
        final byte[] data = unitIdPair.getUnit().array();
        final long id = unitIdPair.getId();
        memoryStore.putMovable(dataClassName, id, codec.decode(data));
      }

    } finally {
      onDataMsgScope.close();
    }
  }

  /**
   * Create a data message using the control message contents, and then
   * send the data message to the correct evaluator.
   */
  private void onCtrlMsg(final AvroElasticMemoryMessage msg) {
    final TraceScope onCtrlMsgScope = Trace.startSpan(ON_CTRL_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()));
    try {

      final CtrlMsg ctrlMsg = msg.getCtrlMsg();

      final String key = ctrlMsg.getDataClassName().toString();
      final Codec codec = serializer.getCodec(key);

      final List<Pair<Long, Object>> idObjectList = new LinkedList<>();
      for (final AvroLongRange avroLongRange : ctrlMsg.getIdRange()) {
        idObjectList.addAll(memoryStore.remove(key, avroLongRange.getMin(), avroLongRange.getMax()));
      }

      final List<UnitIdPair> unitIdPairList = new LinkedList<>();

      for (final Pair<Long, Object> objectId : idObjectList) {
        final UnitIdPair unitIdPair = UnitIdPair.newBuilder()
            .setUnit(ByteBuffer.wrap(codec.encode(objectId.second)))
            .setId(objectId.first)
            .build();

        unitIdPairList.add(unitIdPair);
      }

      sender.get().sendDataMsg(msg.getDestId().toString(), ctrlMsg.getDataClassName().toString(), unitIdPairList,
          TraceInfo.fromSpan(onCtrlMsgScope.getSpan()));

    } finally {
      onCtrlMsgScope.close();
    }
  }
}
