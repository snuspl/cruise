package edu.snu.cay.em.ns;

import edu.snu.cay.em.ns.api.NSWrapper;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * Register and unregister context ids to and from a NSWrapper
 * when contexts spawn/terminate, respectively.
 */
@Unit
public final class NSWrapperContextRegister {

  private NetworkService networkService;
  private IdentifierFactory ifac;

  @Inject
  private NSWrapperContextRegister(final NSWrapper nsWrapper) {
    this.networkService = nsWrapper.getNetworkService();
    this.ifac = this.networkService.getIdentifierFactory();
  }

  public final class RegisterContextHandler implements EventHandler<ContextStart> {
    @Override
    public void onNext(final ContextStart contextStart) {
      networkService.registerId(ifac.getNewInstance(contextStart.getId()));
    }
  }

  public final class UnregisterContextHandler implements EventHandler<ContextStop> {
    @Override
    public void onNext(final ContextStop contextStop) {
      networkService.unregisterId(ifac.getNewInstance(contextStop.getId()));
    }
  }
}
