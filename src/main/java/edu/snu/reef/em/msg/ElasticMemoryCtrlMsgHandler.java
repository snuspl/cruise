package edu.snu.reef.em.msg;

import edu.snu.reef.elastic.memory.serializer.Serializer;
import edu.snu.reef.elastic.memory.task.ElasticMemoryMessageSender;
import edu.snu.reef.elastic.memory.task.MemoryStoreClient;
import org.apache.reef.evaluator.context.ContextMessageHandler;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.util.List;

public final class ElasticMemoryCtrlMsgHandler implements ContextMessageHandler {

  private final Codec<ElasticMemoryCtrlMsg> codec;
  private final ElasticMemoryMessageSender sender;
  private final MemoryStoreClient memoryStoreClient;
  private final Serializer serializer;

  @Inject
  public ElasticMemoryCtrlMsgHandler(final ElasticMemoryCtrlMsgCodec codec,
                                     final ElasticMemoryMessageSender sender,
                                     final MemoryStoreClient memoryStoreClient,
                                     final Serializer serializer) {
    this.codec = codec;
    this.sender = sender;
    this.memoryStoreClient = memoryStoreClient;
    this.serializer = serializer;
  }

  @Override
  public void onNext(final byte[] msg) {
    System.out.println(ElasticMemoryCtrlMsgHandler.class.getSimpleName());
    final ElasticMemoryCtrlMsg decodedMsg = codec.decode(msg);
    System.out.println(decodedMsg);
    System.out.println();

    final String key = decodedMsg.getDataClassName();
    final Codec codec = serializer.getCodec(key);
    System.out.println(codec);
    final List list = memoryStoreClient.get(key);
    memoryStoreClient.remove(key);
    System.out.println(list);
    System.out.println();

    final byte[][] data = new byte[list.size()][];
    for (int index = 0; index < data.length; index++) {
      data[index] = codec.encode(list.get(index));
    }

    sender.send(decodedMsg.getDestId(), decodedMsg.getDataClassName(), data);
  }
}
