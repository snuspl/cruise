package edu.snu.reef.em.msg;

import edu.snu.reef.em.serializer.Serializer;
import edu.snu.reef.em.task.MemoryStoreClient;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public final class ElasticMemoryDataMsgHandler implements EventHandler<ElasticMemoryDataMsg> {
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
  public void onNext(final ElasticMemoryDataMsg msg) {
    LOG.entering(ElasticMemoryDataMsgHandler.class.getSimpleName(), "onNext", msg);

    System.out.println(msg.getFrom());
    System.out.println(msg.getTo());
    System.out.println(msg.getDataClassName());
    final Codec codec = serializer.getCodec(msg.getDataClassName());
    final List list = new LinkedList();
    for (final byte[] data : msg.getData()) {
      System.out.println(codec.decode(data));
      list.add(codec.decode(data));
    }
    list.addAll(memoryStoreClient.get(msg.getDataClassName()));
    System.out.println("Original data is");
    System.out.println(memoryStoreClient.get(msg.getDataClassName()));

    memoryStoreClient.putMovable(msg.getDataClassName(), list);

    LOG.exiting(ElasticMemoryDataMsgHandler.class.getSimpleName(), "onNext", msg);
  }
}
