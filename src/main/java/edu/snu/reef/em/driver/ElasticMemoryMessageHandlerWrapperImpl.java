package edu.snu.reef.em.driver;

import edu.snu.reef.em.avro.AvroElasticMemoryMessage;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Logger;

public final class ElasticMemoryMessageHandlerWrapperImpl implements ElasticMemoryMessageHandlerWrapper {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMessageHandlerWrapperImpl.class.getName());

  private EventHandler<AvroElasticMemoryMessage> elasticMemoryMessageHandler;

  @Inject
  public ElasticMemoryMessageHandlerWrapperImpl() {
  }

  public void setHandler(final EventHandler<AvroElasticMemoryMessage> elasticMemoryMessageEventHandler) {
    this.elasticMemoryMessageHandler = elasticMemoryMessageEventHandler;
  }

  @Override
  public void onNext(final Message<AvroElasticMemoryMessage> msg) {
    boolean foundMessage = false;
    for (final AvroElasticMemoryMessage emMsg : msg.getData()) {
      if (foundMessage) {
        throw new RuntimeException("More than one message was sent");
      }
      foundMessage = true;

      if (elasticMemoryMessageHandler == null) {
        LOG.warning(AvroElasticMemoryMessage.class.getSimpleName() +
            " arrived, but no handler was ready to receive it.");
        continue;
      }

      elasticMemoryMessageHandler.onNext(emMsg);
    }

    if (!foundMessage) {
      throw new RuntimeException("No message was sent");
    }
  }
}
