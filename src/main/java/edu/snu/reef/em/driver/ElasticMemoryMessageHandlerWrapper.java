package edu.snu.reef.em.driver;

import edu.snu.reef.em.avro.AvroElasticMemoryMessage;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

@DefaultImplementation(ElasticMemoryMessageHandlerWrapperImpl.class)
public interface ElasticMemoryMessageHandlerWrapper extends EventHandler<Message<AvroElasticMemoryMessage>> {

  void setHandler(final EventHandler<AvroElasticMemoryMessage> elasticMemoryMessageEventHandler);
}
