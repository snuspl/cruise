package edu.snu.reef.em.task;

import edu.snu.reef.em.ns.NSWrapperClient;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

@Unit
public final class NSWrapperToContext {

  private NetworkService networkService;
  private IdentifierFactory ifac;

  @Inject
  private NSWrapperToContext(final NSWrapperClient nsWrapperClient) {
    this.networkService = nsWrapperClient.getNetworkService();
    this.ifac = this.networkService.getIdentifierFactory();
  }

  public final class BindNSWrapperToContext implements EventHandler<ContextStart> {
    @Override
    public void onNext(final ContextStart contextStart) {
      networkService.registerId(ifac.getNewInstance(contextStart.getId()));
    }
  }

  public final class UnbindNSWrapperToContext implements EventHandler<ContextStop> {
    @Override
    public void onNext(final ContextStop contextStop) {
      networkService.unregisterId(ifac.getNewInstance(contextStop.getId()));
    }
  }
}
