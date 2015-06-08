package edu.snu.reef.elastic.memory.driver;

import edu.snu.reef.em.msg.ElasticMemoryDataMsg;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

public final class ElasticMemoryMessageHandlerWrapper implements EventHandler<Message<ElasticMemoryDataMsg>> {
  private EventHandler<ElasticMemoryDataMsg> elasticMemoryMessageHandler;

  public void addHandler(final EventHandler<ElasticMemoryDataMsg> elasticMemoryMessageEventHandler) {
    this.elasticMemoryMessageHandler = elasticMemoryMessageEventHandler;
  }

  @Override
  public void onNext(final Message<ElasticMemoryDataMsg> msg) {
    if (elasticMemoryMessageHandler == null) {
      throw new RuntimeException("No ElasticMemoryMessageHandler present.");
    }

    boolean foundMessage = false;
    for (final ElasticMemoryDataMsg emMsg : msg.getData()) {
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
