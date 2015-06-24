package edu.snu.reef.em.task;

import edu.snu.reef.em.utils.ElasticMemoryMessageBroadcastHandler;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

@Unit
public final class NSHandlerBind {

  private final ElasticMemoryMessageBroadcastHandler broadcastHandler;
  private final ElasticMemoryMessageBroadcastHandlerEvaluator handlerEvaluator;

  @Inject
  private NSHandlerBind(final ElasticMemoryMessageBroadcastHandler broadcastHandler,
                        final ElasticMemoryMessageBroadcastHandlerEvaluator handlerEvaluator) {
    this.broadcastHandler = broadcastHandler;
    this.handlerEvaluator = handlerEvaluator;
  }

  public final class ContextStartHandler implements EventHandler<ContextStart> {
    @Override
    public void onNext(final ContextStart contextStart) {
      broadcastHandler.addHandler(handlerEvaluator);
    }
  }

  public final class ContextStopHandler implements EventHandler<ContextStop> {
    @Override
    public void onNext(final ContextStop contextStop) {
      broadcastHandler.removeHandler(handlerEvaluator);
    }
  }
}
