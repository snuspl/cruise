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
package edu.snu.cay.services.ps.driver.impl.dynamic;

import edu.snu.cay.services.em.evaluator.api.BlockResolver;
import edu.snu.cay.services.em.evaluator.impl.HashBlockResolver;
import edu.snu.cay.services.ps.common.parameters.NumPartitions;
import edu.snu.cay.services.ps.common.parameters.NumServers;
import edu.snu.cay.services.ps.common.resolver.ServerResolver;
import edu.snu.cay.services.ps.driver.api.ParameterServerManager;
import edu.snu.cay.services.ps.ns.EndpointId;
import edu.snu.cay.services.ps.ns.PSMessageHandler;
import edu.snu.cay.services.ps.server.api.ParameterServer;
import edu.snu.cay.services.ps.server.api.ServerSideReplySender;
import edu.snu.cay.services.ps.server.impl.dynamic.DynamicParameterServer;
import edu.snu.cay.services.ps.server.impl.PartitionedServerSideMsgHandler;
import edu.snu.cay.services.ps.server.impl.ServerSideReplySenderImpl;
import edu.snu.cay.services.ps.server.parameters.ServerNumThreads;
import edu.snu.cay.services.ps.server.parameters.ServerQueueSize;
import edu.snu.cay.services.ps.worker.api.AsyncWorkerHandler;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.services.ps.worker.impl.ParameterWorkerImpl;
import edu.snu.cay.services.ps.worker.impl.dynamic.TaskStartHandler;
import edu.snu.cay.services.ps.worker.impl.ContextStopHandler;
import edu.snu.cay.services.ps.common.resolver.DynamicServerResolver;
import edu.snu.cay.services.ps.worker.impl.AsyncWorkerHandlerImpl;
import edu.snu.cay.services.ps.worker.impl.dynamic.TaskStopHandler;
import edu.snu.cay.services.ps.worker.parameters.ParameterWorkerNumThreads;
import edu.snu.cay.services.ps.worker.parameters.WorkerExpireTimeout;
import edu.snu.cay.services.ps.worker.parameters.WorkerKeyCacheSize;
import edu.snu.cay.services.ps.worker.parameters.WorkerQueueSize;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Manager class for a Dynamic Partitioned Parameter Server, that supports atomic,
 * in-order processing of push and pull operations, running on a single Evaluator.
 * Partitions are logically determined by the Elastic Memory, where each partition consists of
 * disjoint sets of blocks.
 * Each server spawns multiple threads each of which has its individual queue to handle operations.
 *
 * This manager does NOT handle server or worker faults.
 */
@DriverSide
public final class DynamicParameterServerManager implements ParameterServerManager {

  private final int numServers;
  private final int numPartitions;
  private final int workerNumThreads;
  private final int serverNumThreads;
  private final int workerQueueSize;
  private final int serverQueueSize;
  private final long workerExpireTimeout;
  private final int workerKeyCacheSize;

  @Inject
  private DynamicParameterServerManager(@Parameter(NumServers.class)final int numServers,
                                        @Parameter(NumPartitions.class) final int numPartitions,
                                        @Parameter(ParameterWorkerNumThreads.class) final int workerNumThrs,
                                        @Parameter(ServerNumThreads.class) final int serverNumThrs,
                                        @Parameter(WorkerQueueSize.class) final int workerQueueSize,
                                        @Parameter(ServerQueueSize.class) final int serverQueueSize,
                                        @Parameter(WorkerExpireTimeout.class) final long workerExpireTimeout,
                                        @Parameter(WorkerKeyCacheSize.class) final int workerKeyCacheSize) {
    this.numServers = numServers;
    this.numPartitions = numPartitions;
    this.workerNumThreads = workerNumThrs;
    this.serverNumThreads = serverNumThrs;
    this.workerQueueSize = workerQueueSize;
    this.serverQueueSize = serverQueueSize;
    this.workerExpireTimeout = workerExpireTimeout;
    this.workerKeyCacheSize = workerKeyCacheSize;
  }

  /**
   * Returns worker-side service configuration.
   * Sets {@link ParameterWorkerImpl} as the {@link ParameterWorker} class.
   */
  @Override
  public Configuration getWorkerServiceConfiguration(final String contextId) {
    return Tang.Factory.getTang()
        .newConfigurationBuilder(ServiceConfiguration.CONF
            .set(ServiceConfiguration.SERVICES, ParameterWorkerImpl.class)
            .set(ServiceConfiguration.ON_TASK_STARTED, TaskStartHandler.class)
            .set(ServiceConfiguration.ON_TASK_STOP, TaskStopHandler.class)
            .set(ServiceConfiguration.ON_CONTEXT_STOP, ContextStopHandler.class)
            .build())
        .bindImplementation(ParameterWorker.class, ParameterWorkerImpl.class)
        .bindImplementation(AsyncWorkerHandler.class, AsyncWorkerHandlerImpl.class)
        .bindImplementation(ServerResolver.class, DynamicServerResolver.class)
        .bindNamedParameter(NumServers.class, Integer.toString(numServers))
        .bindNamedParameter(NumPartitions.class, Integer.toString(numPartitions))
        .bindNamedParameter(EndpointId.class, contextId)
        .bindNamedParameter(ParameterWorkerNumThreads.class, Integer.toString(workerNumThreads))
        .bindNamedParameter(WorkerQueueSize.class, Integer.toString(workerQueueSize))
        .bindNamedParameter(WorkerExpireTimeout.class, Long.toString(workerExpireTimeout))
        .bindNamedParameter(WorkerKeyCacheSize.class, Integer.toString(workerKeyCacheSize))
        .build();
  }

  /**
   * Returns server-side service configuration.
   */
  @Override
  public Configuration getServerServiceConfiguration(final String contextId) {
    return Configurations.merge(
        Tang.Factory.getTang().newConfigurationBuilder(
            ServiceConfiguration.CONF
                .set(ServiceConfiguration.SERVICES, DynamicParameterServer.class)
                .build())
            .bindImplementation(ParameterServer.class, DynamicParameterServer.class)
            .bindImplementation(ServerSideReplySender.class, ServerSideReplySenderImpl.class)
            .bindNamedParameter(EndpointId.class, contextId)
            .bindNamedParameter(PSMessageHandler.class, PartitionedServerSideMsgHandler.class)
            .bindImplementation(ServerResolver.class, DynamicServerResolver.class)
            .bindNamedParameter(NumServers.class, Integer.toString(numServers))
            .bindNamedParameter(NumPartitions.class, Integer.toString(numPartitions))
            .bindNamedParameter(ServerNumThreads.class, Integer.toString(serverNumThreads))
            .bindNamedParameter(ServerQueueSize.class, Integer.toString(serverQueueSize))
            .bindImplementation(BlockResolver.class, HashBlockResolver.class)
            .build());
  }
}
