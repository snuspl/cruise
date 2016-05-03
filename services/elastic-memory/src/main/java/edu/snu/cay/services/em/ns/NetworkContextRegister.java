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
package edu.snu.cay.services.em.ns;

import edu.snu.cay.services.em.common.parameters.AddedEval;
import edu.snu.cay.services.em.evaluator.impl.OperationRouter;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;

/**
 * Register and unregister context ids to and from a NetworkConnectionService
 * when contexts spawn/terminate, respectively.
 */
@Unit
public final class NetworkContextRegister {

  private final EMNetworkSetup emNetworkSetup;
  private final IdentifierFactory identifierFactory;
  private final OperationRouter router;
  private final ElasticMemoryMsgSender msgSender;

  private final boolean addedEval;

  @Inject
  private NetworkContextRegister(final EMNetworkSetup emNetworkSetup,
                                 final IdentifierFactory identifierFactory,
                                 final OperationRouter router,
                                 final ElasticMemoryMsgSender msgSender,
                                 @Parameter(AddedEval.class) final boolean addedEval) {
    this.emNetworkSetup = emNetworkSetup;
    this.identifierFactory = identifierFactory;
    this.router = router;
    this.msgSender = msgSender;
    this.addedEval = addedEval;
  }

  public final class RegisterContextHandler implements EventHandler<ContextStart> {
    @Override
    public void onNext(final ContextStart contextStart) {
      final Identifier identifier = identifierFactory.getNewInstance(contextStart.getId());
      emNetworkSetup.registerConnectionFactory(identifier);

      // request the info of router initialization to driver, when the evaluator is dynamically added by EM.add()
      if (addedEval) {
        try (final TraceScope traceScope = Trace.startSpan("ROUTING_INIT_REQUEST")) {
          final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());
          msgSender.sendRoutingInitRequestMsg(traceInfo);
        }
      } else {
        final String localId = emNetworkSetup.getMyId().toString();
        router.initialize(localId);
      }
    }
  }

  public final class UnregisterContextHandler implements EventHandler<ContextStop> {
    @Override
    public void onNext(final ContextStop contextStop) {
      emNetworkSetup.unregisterConnectionFactory();
    }
  }
}
