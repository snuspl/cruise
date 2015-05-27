package edu.snu.reef.elastic.memory;

import edu.snu.reef.elastic.memory.task.MemoryStoreClient;
import edu.snu.reef.examples.parameters.ExampleKey;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Logger;

public final class ElasticMemoryMessageHandler implements EventHandler<ElasticMemoryMessage> {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMessageHandler.class.getName());

  private final MemoryStoreClient memoryStoreClient;

  @Inject
  public ElasticMemoryMessageHandler(final MemoryStoreClient memoryStoreClient) {
    this.memoryStoreClient = memoryStoreClient;
  }

  @Override
  public void onNext(final ElasticMemoryMessage msg) {
    LOG.entering(ElasticMemoryMessageHandler.class.getSimpleName(), "onNext", msg);
    System.out.println(memoryStoreClient.get(ExampleKey.class));

    System.out.println(msg);
    LOG.exiting(ElasticMemoryMessageHandler.class.getSimpleName(), "onNext", msg);
  }
}
