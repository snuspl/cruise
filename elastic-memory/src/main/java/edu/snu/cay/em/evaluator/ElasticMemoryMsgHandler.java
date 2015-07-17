package edu.snu.cay.em.evaluator;

import edu.snu.cay.em.avro.*;
import edu.snu.cay.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.em.serialize.Serializer;
import edu.snu.cay.em.utils.SingleMessageExtractor;
import edu.snu.cay.em.evaluator.api.MemoryStore;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;
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

  /**
   * Create a data message using the control message contents, and then
   * send the data message to the correct evaluator.
   */
  private void onCtrlMsg(final AvroElasticMemoryMessage msg) {
    final CtrlMsg ctrlMsg = msg.getCtrlMsg();

    final String key = ctrlMsg.getDataClassName().toString();
    final Codec codec = serializer.getCodec(key);

    final List list = memoryStore.get(key);
    memoryStore.remove(key);

    final List<UnitIdPair> unitIdPairList = new LinkedList<>();

    // TODO: Currently send meaningless values for ids. Must fix.
    for (final Object object : list) {
      final UnitIdPair unitIdPair = UnitIdPair.newBuilder()
          .setUnit(ByteBuffer.wrap(codec.encode(object)))
          .setId(0)
          .build();

      unitIdPairList.add(unitIdPair);
    }

    sender.get().sendDataMsg(msg.getDestId().toString(), ctrlMsg.getDataClassName().toString(), unitIdPairList);
  }
}
