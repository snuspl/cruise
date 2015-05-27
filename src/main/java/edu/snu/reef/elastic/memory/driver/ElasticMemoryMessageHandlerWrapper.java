package edu.snu.reef.elastic.memory.driver;

import edu.snu.reef.elastic.memory.ElasticMemoryMessage;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

public final class ElasticMemoryMessageHandlerWrapper implements EventHandler<Message<ElasticMemoryMessage>> {
  private EventHandler<ElasticMemoryMessage> elasticMemoryMessageHandler;

  public void addHandler(final EventHandler<ElasticMemoryMessage> elasticMemoryMessageEventHandler) {
    this.elasticMemoryMessageHandler = elasticMemoryMessageEventHandler;
  }

  @Override
  public void onNext(final Message<ElasticMemoryMessage> msg) {
    if (elasticMemoryMessageHandler == null) {
      throw new RuntimeException("No ElasticMemoryMessageHandler present.");
    }

    boolean foundMessage = false;
    for (final ElasticMemoryMessage emMsg : msg.getData()) {
      if (foundMessage) {
        throw new RuntimeException("More than one message was sent");
      }

      foundMessage = true;
      elasticMemoryMessageHandler.onNext(emMsg);
    }

    if (!foundMessage) {
      throw new RuntimeException("No message was sent");
    }
  }
}
