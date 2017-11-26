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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.services.et.configuration.parameters.ExecutorStart;
import edu.snu.cay.services.et.configuration.parameters.ExecutorStartHandlers;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Set;

/**
 * Created by xyzi on 27/11/2017.
 */
public final class ExecutorRuntime {

  @Inject
  private ExecutorRuntime(final Tables tables,
                          final Tasklets tasklets,
                          @Parameter(ExecutorIdentifier.class) final String executorId,
                          @Parameter(ExecutorStartHandlers.class)
                            final Set<EventHandler<ExecutorStart>> executorStartHandlers) {
    final ExecutorStart executorStart = new ExecutorStart(executorId);
    executorStartHandlers.forEach(executorStartEventHandler -> executorStartEventHandler.onNext(executorStart));
  }
}
