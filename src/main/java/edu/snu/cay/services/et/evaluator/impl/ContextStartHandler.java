/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.common.api.NetworkConnection;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.runtime.common.evaluator.parameters.EvaluatorIdentifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * Handles initial setup of container.
 */
@Private
@EvaluatorSide
public final class ContextStartHandler implements EventHandler<ContextStart> {
  private final NetworkConnection networkConnection;
  private final Identifier evaluatorIdentifier;

  @Inject
  private ContextStartHandler(final NetworkConnection networkConnection,
                              final IdentifierFactory identifierFactory,
                              @Parameter(EvaluatorIdentifier.class) final String evaluatorIdentifier) {
    this.networkConnection = networkConnection;
    this.evaluatorIdentifier = identifierFactory.getNewInstance(evaluatorIdentifier);
  }

  @Override
  public void onNext(final ContextStart contextStart) {
    networkConnection.setup(evaluatorIdentifier);
  }
}
