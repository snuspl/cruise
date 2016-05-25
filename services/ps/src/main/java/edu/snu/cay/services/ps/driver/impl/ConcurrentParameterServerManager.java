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
package edu.snu.cay.services.ps.driver.impl;

import edu.snu.cay.services.ps.driver.api.ParameterServerManager;
import edu.snu.cay.services.ps.ns.EndpointId;
import edu.snu.cay.services.ps.ns.PSMessageHandler;
import edu.snu.cay.services.ps.server.concurrent.api.ParameterServer;
import edu.snu.cay.services.ps.server.concurrent.impl.ServerSideMsgHandler;
import edu.snu.cay.services.ps.server.concurrent.impl.ConcurrentParameterServer;
import edu.snu.cay.services.ps.worker.AsyncWorkerHandler;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.services.ps.worker.concurrent.ConcurrentParameterWorker;
import edu.snu.cay.services.ps.worker.concurrent.ConcurrentWorkerHandler;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;

import javax.inject.Inject;

import static edu.snu.cay.services.ps.common.Constants.SERVER_ID_PREFIX;

/**
 * Manager class for a Parameter Server that uses only one node for a server.
 * This manager does NOT handle server or worker faults.
 */
@DriverSide
public final class ConcurrentParameterServerManager implements ParameterServerManager {

  @Inject
  private ConcurrentParameterServerManager() {
  }

  /**
   * Returns worker-side service configuration.
   * Sets {@link ConcurrentParameterWorker} as the {@link ParameterWorker} class.
   */
  @Override
  public Configuration getWorkerServiceConfiguration(final String contextId) {
    return Tang.Factory.getTang()
        .newConfigurationBuilder(ServiceConfiguration.CONF
            .set(ServiceConfiguration.SERVICES, ConcurrentParameterWorker.class)
            .build())
        .bindImplementation(ParameterWorker.class, ConcurrentParameterWorker.class)
        .bindImplementation(AsyncWorkerHandler.class, ConcurrentWorkerHandler.class)
        .bindNamedParameter(ServerId.class, SERVER_ID_PREFIX + 0)
        .bindNamedParameter(EndpointId.class, contextId)
        .build();
  }

  /**
   * Returns server-side service configuration.
   * Sets {@link ConcurrentParameterServer} as the {@link ParameterServer} class.
   */
  @Override
  public Configuration getServerServiceConfiguration(final String contextId) {
    return Tang.Factory.getTang()
        .newConfigurationBuilder(ServiceConfiguration.CONF
            .set(ServiceConfiguration.SERVICES, ConcurrentParameterServer.class)
            .build())
        .bindNamedParameter(PSMessageHandler.class, ServerSideMsgHandler.class)
        .bindImplementation(ParameterServer.class, ConcurrentParameterServer.class)
        .bindNamedParameter(EndpointId.class, SERVER_ID_PREFIX + 0)
        .build();
  }

}
