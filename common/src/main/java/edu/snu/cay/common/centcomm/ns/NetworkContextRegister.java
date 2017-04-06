/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.common.centcomm.ns;

import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * Register and unregister identifiers to and from a NetworkConnectionService
 * when contexts spawn/terminate, respectively.
 */
@Unit
public final class NetworkContextRegister {
  private final CentCommNetworkSetup centCommNetworkSetup;
  private final IdentifierFactory identifierFactory;

  @Inject
  private NetworkContextRegister(final CentCommNetworkSetup centCommNetworkSetup,
                                 final IdentifierFactory identifierFactory) {
    this.centCommNetworkSetup = centCommNetworkSetup;
    this.identifierFactory = identifierFactory;
  }

  public final class RegisterContextHandler implements EventHandler<ContextStart> {
    @Override
    public void onNext(final ContextStart contextStart) {
      centCommNetworkSetup.registerConnectionFactory(identifierFactory.getNewInstance(contextStart.getId()));
    }
  }

  public final class UnregisterContextHandler implements EventHandler<ContextStop> {
    @Override
    public void onNext(final ContextStop contextStop) {
      centCommNetworkSetup.unregisterConnectionFactory();
    }
  }
}
