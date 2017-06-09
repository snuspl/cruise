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
package edu.snu.cay.dolphin.async.network;

import edu.snu.cay.dolphin.async.WorkerSideMsgHandler;
import org.apache.reef.evaluator.context.parameters.ContextStartHandlers;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;

import javax.inject.Inject;

/**
 * Created by cmslab on 6/8/17.
 */
public final class NetworkConfProvider {

  @Inject
  private NetworkConfProvider() {
  }

  public Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextStartHandlers.class, ContextStartHandler.class)
        .build();
  }

  public Configuration getServiceConfiguration(final String jobId) {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(JobIdentifier.class, jobId)
        .bindImplementation(MessageHandler.class, WorkerSideMsgHandler.class)
        .build();
  }
}
