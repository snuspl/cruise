package edu.snu.cay.services.em.ns;

import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * Register and unregister context ids to and from a NSWrapper
 * when contexts spawn/terminate, respectively.
 */
@Unit
public final class EMNetworkContextRegister {

  private NetworkConnectionService networkConnectionService;
  private IdentifierFactory identifierFactory;

  @Inject
  private EMNetworkContextRegister(final NetworkConnectionService networkConnectionService,
                                   final IdentifierFactory identifierFactory) {
    this.networkConnectionService = networkConnectionService;
    this.identifierFactory = identifierFactory;
  }

  public final class RegisterContextHandler implements EventHandler<ContextStart> {
    @Override
    public void onNext(final ContextStart contextStart) {
      networkConnectionService.registerId(identifierFactory.getNewInstance(contextStart.getId()));
    }
  }

  public final class UnregisterContextHandler implements EventHandler<ContextStop> {
    @Override
    public void onNext(final ContextStop contextStop) {
      networkConnectionService.unregisterId(identifierFactory.getNewInstance(contextStop.getId()));
    }
  }
}
