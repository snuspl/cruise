package edu.snu.reef.em.driver;

import edu.snu.reef.em.msg.ElasticMemoryDataMsg;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

@DefaultImplementation(ElasticMemoryMessageHandlerWrapperImpl.class)
public interface ElasticMemoryMessageHandlerWrapper extends EventHandler<Message<ElasticMemoryDataMsg>> {

  void setHandler(final EventHandler<ElasticMemoryDataMsg> elasticMemoryMessageEventHandler);
}
