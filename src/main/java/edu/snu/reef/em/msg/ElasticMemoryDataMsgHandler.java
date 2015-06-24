package edu.snu.reef.em.msg;

import edu.snu.reef.em.avro.AvroElasticMemoryMessage;
import edu.snu.reef.em.avro.DataMsg;
import edu.snu.reef.em.avro.Type;
import edu.snu.reef.em.avro.UnitIdPair;
import edu.snu.reef.em.serializer.Serializer;
import edu.snu.reef.em.task.MemoryStoreClient;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public final class ElasticMemoryDataMsgHandler implements EventHandler<AvroElasticMemoryMessage> {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryDataMsgHandler.class.getName());

  private final MemoryStoreClient memoryStoreClient;
  private final Serializer serializer;

  @Inject
  public ElasticMemoryDataMsgHandler(final MemoryStoreClient memoryStoreClient,
                                     final Serializer serializer) {
    this.memoryStoreClient = memoryStoreClient;
    this.serializer = serializer;
  }

  @Override
  public void onNext(final AvroElasticMemoryMessage msg) {
    LOG.entering(AvroElasticMemoryMessage.class.getSimpleName(), "onNext", msg);

    System.out.println("Message source: " + msg.getSrcId());
    System.out.println("Message destination: " + msg.getDestId());
    System.out.println("Message type: " + msg.getType());
    assert(msg.getType() == Type.DataMsg);

    final DataMsg dataMsg = msg.getDataMsg();

    final Codec codec = serializer.getCodec(dataMsg.getDataClassName().toString());
    final List list = new LinkedList();
    for (final UnitIdPair unitIdPair : dataMsg.getUnits()) {
      final byte[] data = unitIdPair.getUnit().array();
      System.out.println(codec.decode(data));
      list.add(codec.decode(data));
    }
    list.addAll(memoryStoreClient.get(dataMsg.getDataClassName().toString()));
    System.out.println("Original data is");
    System.out.println(memoryStoreClient.get(dataMsg.getDataClassName().toString()));

    memoryStoreClient.putMovable(dataMsg.getDataClassName().toString(), list);

    LOG.exiting(AvroElasticMemoryMessage.class.getSimpleName(), "onNext", msg);
  }
}
