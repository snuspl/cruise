package edu.snu.reef.em.msg;

import edu.snu.reef.em.avro.*;
import edu.snu.reef.em.serializer.Serializer;
import edu.snu.reef.em.task.ElasticMemoryMessageSender;
import edu.snu.reef.em.task.MemoryStore;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public final class ElasticMemoryDataMsgHandler implements EventHandler<AvroElasticMemoryMessage> {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryDataMsgHandler.class.getName());

  private final MemoryStore memoryStore;
  private final Serializer serializer;
  private final ElasticMemoryMessageSender sender;

  @Inject
  public ElasticMemoryDataMsgHandler(final MemoryStore memoryStore,
                                     final ElasticMemoryMessageSender sender,
                                     final Serializer serializer) {
    this.memoryStore = memoryStore;
    this.serializer = serializer;
    this.sender = sender;
  }

  @Override
  public void onNext(final AvroElasticMemoryMessage msg) {
    LOG.entering(AvroElasticMemoryMessage.class.getSimpleName(), "onNext", msg);

    System.out.println("Message source: " + msg.getSrcId());
    System.out.println("Message destination: " + msg.getDestId());
    System.out.println("Message type: " + msg.getType());
    switch (msg.getType()) {
      case DataMsg:
        onDataMsg(msg);
        break;

      case CtrlMsg:
        onCtrlMsg(msg);
        break;

      default:
        throw new RuntimeException("Not Excepted: msg");
    }



    LOG.exiting(AvroElasticMemoryMessage.class.getSimpleName(), "onNext", msg);
  }

  private void onDataMsg(final AvroElasticMemoryMessage msg) {
    final DataMsg dataMsg = msg.getDataMsg();

    final Codec codec = serializer.getCodec(dataMsg.getDataClassName().toString());
    final List list = new LinkedList();
    for (final UnitIdPair unitIdPair : dataMsg.getUnits()) {
      final byte[] data = unitIdPair.getUnit().array();
      System.out.println(codec.decode(data));
      list.add(codec.decode(data));
    }
    list.addAll(memoryStore.get(dataMsg.getDataClassName().toString()));
    System.out.println("Original data is");
    System.out.println(memoryStore.get(dataMsg.getDataClassName().toString()));

    memoryStore.putMovable(dataMsg.getDataClassName().toString(), list);
  }

  private void onCtrlMsg(final AvroElasticMemoryMessage msg) {
    final CtrlMsg ctrlMsg = msg.getCtrlMsg();

    final String key = ctrlMsg.getDataClassName().toString();
    final Codec codec = serializer.getCodec(key);
    System.out.println(codec);

    final List list = memoryStore.get(key);
    memoryStore.remove(key);
    System.out.println(list);
    System.out.println();

    final List<UnitIdPair> unitIdPairList = new LinkedList<>();

    for (int index = 0; index < list.size(); index++) {
      final UnitIdPair unitIdPair = UnitIdPair.newBuilder()
          .setUnit(ByteBuffer.wrap(codec.encode(list.get(index))))
          .setId(0)
          .build();

      unitIdPairList.add(unitIdPair);
    }

    sender.send(msg.getDestId().toString(), ctrlMsg.getDataClassName().toString(), unitIdPairList);
  }
}
