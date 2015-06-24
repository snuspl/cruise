package edu.snu.reef.em.driver;

import edu.snu.reef.em.avro.AvroElasticMemoryMessage;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

public final class ElasticMemoryMessageHandlerWrapperImpl implements ElasticMemoryMessageHandlerWrapper {

  private EventHandler<AvroElasticMemoryMessage> elasticMemoryMessageHandler;

  @Inject
  private ElasticMemoryMessageHandlerWrapperImpl() {
  }

  public void setHandler(final EventHandler<AvroElasticMemoryMessage> elasticMemoryMessageEventHandler) {
    this.elasticMemoryMessageHandler = elasticMemoryMessageEventHandler;
  }

  @Override
  public void onNext(final Message<AvroElasticMemoryMessage> msg) {
    if (elasticMemoryMessageHandler == null) {
      throw new RuntimeException("No ElasticMemoryMessageHandler present.");
    }

    boolean foundMessage = false;
    for (final AvroElasticMemoryMessage emMsg : msg.getData()) {
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
