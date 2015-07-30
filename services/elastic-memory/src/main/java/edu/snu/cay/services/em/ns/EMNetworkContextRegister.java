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

  // TODO: no need for this if networkConnectionService.getMyId() is available
  private final EMNetworkSetup emNetworkSetup;
  private final NetworkConnectionService networkConnectionService;
  private final IdentifierFactory identifierFactory;

  @Inject
  private EMNetworkContextRegister(final EMNetworkSetup emNetworkSetup,
                                   final NetworkConnectionService networkConnectionService,
                                   final IdentifierFactory identifierFactory) {
    this.emNetworkSetup = emNetworkSetup;
    this.networkConnectionService = networkConnectionService;
    this.identifierFactory = identifierFactory;
  }

  public final class RegisterContextHandler implements EventHandler<ContextStart> {
    @Override
    public void onNext(final ContextStart contextStart) {
      networkConnectionService.registerId(identifierFactory.getNewInstance(contextStart.getId()));
      emNetworkSetup.setMyId(identifierFactory.getNewInstance(contextStart.getId()));
    }
  }

  public final class UnregisterContextHandler implements EventHandler<ContextStop> {
    @Override
    public void onNext(final ContextStop contextStop) {
      networkConnectionService.unregisterId(identifierFactory.getNewInstance(contextStop.getId()));
      emNetworkSetup.setMyId(identifierFactory.getNewInstance(contextStop.getId()));
    }
  }
}
