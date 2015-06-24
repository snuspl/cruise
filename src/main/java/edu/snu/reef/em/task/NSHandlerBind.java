package edu.snu.reef.em.task;

import edu.snu.reef.em.utils.ElasticMemoryMessageBroadcaster;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

@Unit
public final class NSHandlerBind {

  private final ElasticMemoryMessageBroadcaster broadcaster;
  private final ElasticMemoryMessageBroadcastHandlerEvaluator handlerEvaluator;

  @Inject
  private NSHandlerBind(final ElasticMemoryMessageBroadcaster broadcaster,
                        final ElasticMemoryMessageBroadcastHandlerEvaluator handlerEvaluator) {
    this.broadcaster = broadcaster;
    this.handlerEvaluator = handlerEvaluator;
  }

  public final class ContextStartHandler implements EventHandler<ContextStart> {
    @Override
    public void onNext(final ContextStart contextStart) {
      broadcaster.addHandler(handlerEvaluator);
    }
  }

  public final class ContextStopHandler implements EventHandler<ContextStop> {
    @Override
    public void onNext(final ContextStop contextStop) {
      broadcaster.removeHandler(handlerEvaluator);
    }
  }
}
