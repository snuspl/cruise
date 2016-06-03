/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.services.ps.ns;

import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Register and unregister identifiers to and from a NetworkConnectionService
 * when contexts spawn/terminate, respectively.
 */
public final class NetworkContextRegister {
  private static final Logger LOG = Logger.getLogger(NetworkContextRegister.class.getName());

  public static final class RegisterContextHandler implements EventHandler<ContextStart> {
    private final PSNetworkSetup psNetworkSetup;
    private final IdentifierFactory identifierFactory;

    @Inject
    private RegisterContextHandler(final PSNetworkSetup psNetworkSetup,
                                   final IdentifierFactory identifierFactory) {
      this.psNetworkSetup = psNetworkSetup;
      this.identifierFactory = identifierFactory;
    }

    @Override
    public void onNext(final ContextStart contextStart) {
      psNetworkSetup.registerConnectionFactory(identifierFactory.getNewInstance(contextStart.getId()));
      LOG.log(Level.INFO, "My NCS id is " + psNetworkSetup.getMyId());
    }
  }

  /**
   * ContextStop handler for PS servers.
   */
  public static final class UnregisterContextHandlerForServer implements EventHandler<ContextStop> {
    private final PSNetworkSetup psNetworkSetup;

    @Inject
    private UnregisterContextHandlerForServer(final PSNetworkSetup psNetworkSetup) {
      this.psNetworkSetup = psNetworkSetup;
    }

    @Override
    public void onNext(final ContextStop contextStop) {
      // we can close servers before unregistering it from NCS just like workers
      // but it's not necessary until now
      psNetworkSetup.unregisterConnectionFactory();
    }
  }

  /**
   * ContextStop handler for PS workers.
   * It guarantees PS service to be closed after fully sending out all queued operations.
   * With this guarantee, the context can be shutdown, minimizing message loss.
   *
   * However, we are still observing some lost messages when
   * contexts are immediately closed after the task completes.
   * It appears messages buffered in NCS are not being flushed before context close,
   * but this has to be investigated further.
   */
  public static final class UnregisterContextHandlerForWorker implements EventHandler<ContextStop> {
    private static final long TIMEOUT_MS = 10000;

    private final PSNetworkSetup psNetworkSetup;
    private final ParameterWorker parameterWorker;

    @Inject
    private UnregisterContextHandlerForWorker(final PSNetworkSetup psNetworkSetup,
                                              final ParameterWorker parameterWorker) {
      this.psNetworkSetup = psNetworkSetup;
      this.parameterWorker = parameterWorker;
    }

    @Override
    public void onNext(final ContextStop contextStop) {
      LOG.log(Level.INFO, "Wait {0} milliseconds for the PS service to be closed", TIMEOUT_MS);
      if (parameterWorker.close(TIMEOUT_MS)) {
        LOG.info("Succeed to close PS worker cleanly");
      } else {
        LOG.info("Fail to close PS worker cleanly");
      }

      psNetworkSetup.unregisterConnectionFactory();
    }
  }
}
