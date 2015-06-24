package edu.snu.reef.em.evaluator;

import edu.snu.reef.em.msg.ElasticMemoryMsgBroadcaster;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * Adds ElasticMemoryMsgHandlerEvaluator to ElasticMemoryMsgBroadcaster so that
 * ElasticMemoryMsgHandlerEvaluator can successfully receive messages.
 */
@EvaluatorSide
@Unit
public final class ElasticMemoryMsgHandlerBinder {

  private final ElasticMemoryMsgBroadcaster broadcaster;
  private final ElasticMemoryMsgHandlerEvaluator handlerEvaluator;

  @Inject
  private ElasticMemoryMsgHandlerBinder(final ElasticMemoryMsgBroadcaster broadcaster,
                                        final ElasticMemoryMsgHandlerEvaluator handlerEvaluator) {
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
