package edu.snu.reef.em.task;

import edu.snu.reef.em.driver.ElasticMemoryMessageHandlerWrapper;
import edu.snu.reef.em.msg.ElasticMemoryDataMsgHandler;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

public class ElasticMemoryClient implements EventHandler<ContextStart> {

  private final ElasticMemoryMessageHandlerWrapper emMsgHandlerWrapper;
  private final ElasticMemoryDataMsgHandler emDataMsgHandler;

  @Inject
  private ElasticMemoryClient(final ElasticMemoryMessageHandlerWrapper emMsgHandlerWrapper,
                              final ElasticMemoryDataMsgHandler emDataMsgHandler) {
    this.emMsgHandlerWrapper = emMsgHandlerWrapper;
    this.emDataMsgHandler = emDataMsgHandler;
  }

  @Override
  public void onNext(final ContextStart contextStart) {
    emMsgHandlerWrapper.setHandler(emDataMsgHandler);
  }
}
