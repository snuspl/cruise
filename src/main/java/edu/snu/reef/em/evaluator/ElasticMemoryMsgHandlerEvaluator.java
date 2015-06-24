package edu.snu.reef.em.evaluator;

import edu.snu.reef.em.avro.AvroElasticMemoryMessage;
import edu.snu.reef.em.avro.CtrlMsg;
import edu.snu.reef.em.avro.DataMsg;
import edu.snu.reef.em.avro.UnitIdPair;
import edu.snu.reef.em.evaluator.api.MemoryStore;
import edu.snu.reef.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.reef.em.serialize.Serializer;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Represents the evaluator-side message handler for receiving ElasticMemoryMessages.
 */
@EvaluatorSide
public final class ElasticMemoryMsgHandlerEvaluator implements EventHandler<AvroElasticMemoryMessage> {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMsgHandlerEvaluator.class.getName());

  private final MemoryStore memoryStore;
  private final Serializer serializer;
  private final ElasticMemoryMsgSender sender;

  @Inject
  private ElasticMemoryMsgHandlerEvaluator(final MemoryStore memoryStore,
                                           final ElasticMemoryMsgSender sender,
                                           final Serializer serializer) {
    this.memoryStore = memoryStore;
    this.serializer = serializer;
    this.sender = sender;
  }

  @Override
  public void onNext(final AvroElasticMemoryMessage msg) {
    LOG.entering(ElasticMemoryMsgHandlerEvaluator.class.getSimpleName(), "onNext", msg);

    switch (msg.getType()) {
      case DataMsg:
        onDataMsg(msg);
        break;

      case CtrlMsg:
        onCtrlMsg(msg);
        break;

      default:
        throw new RuntimeException("Unexpected message: " + msg);
    }

    LOG.exiting(ElasticMemoryMsgHandlerEvaluator.class.getSimpleName(), "onNext", msg);
  }

  private void onDataMsg(final AvroElasticMemoryMessage msg) {
    final DataMsg dataMsg = msg.getDataMsg();

    final Codec codec = serializer.getCodec(dataMsg.getDataClassName().toString());
    final List list = new LinkedList();
    for (final UnitIdPair unitIdPair : dataMsg.getUnits()) {
      final byte[] data = unitIdPair.getUnit().array();
      list.add(codec.decode(data));
    }
    list.addAll(memoryStore.get(dataMsg.getDataClassName().toString()));

    memoryStore.putMovable(dataMsg.getDataClassName().toString(), list);
  }

  private void onCtrlMsg(final AvroElasticMemoryMessage msg) {
    final CtrlMsg ctrlMsg = msg.getCtrlMsg();

    final String key = ctrlMsg.getDataClassName().toString();
    final Codec codec = serializer.getCodec(key);

    final List list = memoryStore.get(key);
    memoryStore.remove(key);

    final List<UnitIdPair> unitIdPairList = new LinkedList<>();

    // TODO: Currently end meaningless values for ids. Must fix.
    for (int index = 0; index < list.size(); index++) {
      final UnitIdPair unitIdPair = UnitIdPair.newBuilder()
          .setUnit(ByteBuffer.wrap(codec.encode(list.get(index))))
          .setId(0)
          .build();

      unitIdPairList.add(unitIdPair);
    }

    sender.sendDataMsg(msg.getDestId().toString(), ctrlMsg.getDataClassName().toString(), unitIdPairList);
  }
}
