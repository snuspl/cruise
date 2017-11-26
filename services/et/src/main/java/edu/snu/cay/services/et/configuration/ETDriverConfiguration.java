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
package edu.snu.cay.services.et.configuration;

import edu.snu.cay.services.et.common.api.MessageHandler;
import edu.snu.cay.services.et.configuration.parameters.ETIdentifier;
import edu.snu.cay.services.et.configuration.parameters.chkp.ChkpCommitPath;
import edu.snu.cay.services.et.configuration.parameters.chkp.ChkpTempPath;
import edu.snu.cay.services.et.driver.impl.*;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.*;
import org.apache.reef.tang.formats.*;
import org.apache.reef.wake.EventHandler;

/**
 * A builder for ET's driver-side configuration.
 */
public final class ETDriverConfiguration extends ConfigurationModuleBuilder {
  public static final RequiredImpl<EventHandler<AllocatedEvaluator>> ON_EVALUATOR_ALLOCATED = new RequiredImpl<>();
  public static final RequiredImpl<EventHandler<CompletedEvaluator>> ON_EVALUATOR_COMPLETED = new RequiredImpl<>();
  public static final RequiredImpl<EventHandler<FailedEvaluator>> ON_EVALUATOR_FAILED = new RequiredImpl<>();
  public static final RequiredImpl<EventHandler<ActiveContext>> ON_CONTEXT_ACTIVE = new RequiredImpl<>();
  public static final OptionalParameter<String> ET_IDENTIFIER = new OptionalParameter<>();
  public static final OptionalParameter<String> CHKP_TEMP_PATH = new OptionalParameter<>();
  public static final OptionalParameter<String> CHKP_COMMIT_PATH = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new ETDriverConfiguration()
      .bindSetEntry(EvaluatorAllocatedHandlers.class, ON_EVALUATOR_ALLOCATED)
      .bindSetEntry(EvaluatorCompletedHandlers.class, ON_EVALUATOR_COMPLETED)
      .bindSetEntry(EvaluatorFailedHandlers.class, ON_EVALUATOR_FAILED)
      .bindSetEntry(ContextActiveHandlers.class, ON_CONTEXT_ACTIVE)
      .bindImplementation(MessageHandler.class, MessageHandlerImpl.class)
      .bindNamedParameter(ETIdentifier.class, ET_IDENTIFIER)
      .bindNamedParameter(ChkpTempPath.class, CHKP_TEMP_PATH)
      .bindNamedParameter(ChkpCommitPath.class, CHKP_COMMIT_PATH)
      .build()
      .set(ON_EVALUATOR_ALLOCATED, EvaluatorAllocatedHandler.class)
      .set(ON_EVALUATOR_COMPLETED, EvaluatorCompletedHandler.class)
      .set(ON_EVALUATOR_FAILED, EvaluatorFailedHandler.class)
      .set(ON_CONTEXT_ACTIVE, ContextActiveHandler.class);
}
