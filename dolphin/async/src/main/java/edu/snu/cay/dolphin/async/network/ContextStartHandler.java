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
package edu.snu.cay.dolphin.async.network;



import edu.snu.cay.dolphin.async.DolphinMsg;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.runtime.common.evaluator.parameters.EvaluatorIdentifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * Handles initial network setup of a executor.
 */
@Private
@EvaluatorSide
public final class ContextStartHandler implements EventHandler<ContextStart> {
  private final NetworkConnection<DolphinMsg> networkConnection;
  private final String jobId;
  private final String evaluatorId;

  @Inject
  private ContextStartHandler(final NetworkConnection<DolphinMsg> networkConnection,
                              @Parameter(JobIdentifier.class) final String jobId,
                              @Parameter(EvaluatorIdentifier.class) final String evaluatorId) {
    this.networkConnection = networkConnection;
    this.jobId = jobId;
    this.evaluatorId = evaluatorId;
  }

  @Override
  public void onNext(final ContextStart contextStart) {
    networkConnection.setup(jobId, evaluatorId);
  }
}
