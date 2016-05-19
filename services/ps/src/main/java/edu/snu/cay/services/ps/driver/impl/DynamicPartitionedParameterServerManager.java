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
package edu.snu.cay.services.ps.driver.impl;

import edu.snu.cay.services.em.evaluator.api.BlockResolver;
import edu.snu.cay.services.em.evaluator.impl.HashBlockResolver;
import edu.snu.cay.services.ps.common.partitioned.parameters.NumPartitions;
import edu.snu.cay.services.ps.common.partitioned.parameters.NumServers;
import edu.snu.cay.services.ps.common.partitioned.resolver.ServerResolver;
import edu.snu.cay.services.ps.driver.api.ParameterServerManager;
import edu.snu.cay.services.ps.ns.EndpointId;
import edu.snu.cay.services.ps.ns.PSMessageHandler;
import edu.snu.cay.services.ps.server.partitioned.*;
import edu.snu.cay.services.ps.server.partitioned.parameters.ServerNumThreads;
import edu.snu.cay.services.ps.server.partitioned.parameters.ServerQueueSize;
import edu.snu.cay.services.ps.worker.AsyncWorkerHandler;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.services.ps.worker.partitioned.dynamic.TaskStartHandler;
import edu.snu.cay.services.ps.worker.partitioned.ContextStopHandler;
import edu.snu.cay.services.ps.common.partitioned.resolver.DynamicServerResolver;
import edu.snu.cay.services.ps.worker.partitioned.PartitionedParameterWorker;
import edu.snu.cay.services.ps.worker.partitioned.PartitionedWorkerHandler;
import edu.snu.cay.services.ps.worker.partitioned.dynamic.TaskStopHandler;
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
public final class DynamicPartitionedParameterServerManager implements ParameterServerManager {

  private final int numServers;
  private final int numPartitions;
  private final int serverNumThreads;
  private final int queueSize;

  @Inject
  private DynamicPartitionedParameterServerManager(@Parameter(NumServers.class)final int numServers,
                                                   @Parameter(NumPartitions.class) final int numPartitions,
                                                   @Parameter(ServerNumThreads.class) final int serverNumThreads,
                                                   @Parameter(ServerQueueSize.class) final int queueSize) {
    this.numServers = numServers;
    this.numPartitions = numPartitions;
    this.serverNumThreads = serverNumThreads;
    this.queueSize = queueSize;
  }

  /**
   * Returns worker-side service configuration.
   * Sets {@link PartitionedParameterWorker} as the {@link ParameterWorker} class.
   */
  @Override
  public Configuration getWorkerServiceConfiguration(final String contextId) {
    return Tang.Factory.getTang()
        .newConfigurationBuilder(ServiceConfiguration.CONF
            .set(ServiceConfiguration.SERVICES, PartitionedParameterWorker.class)
            .set(ServiceConfiguration.ON_TASK_STARTED, TaskStartHandler.class)
            .set(ServiceConfiguration.ON_TASK_STOP, TaskStopHandler.class)
            .set(ServiceConfiguration.ON_CONTEXT_STOP, ContextStopHandler.class)
            .build())
        .bindImplementation(ParameterWorker.class, PartitionedParameterWorker.class)
        .bindImplementation(AsyncWorkerHandler.class, PartitionedWorkerHandler.class)
        .bindImplementation(ServerResolver.class, DynamicServerResolver.class)
        .bindNamedParameter(NumServers.class, Integer.toString(numServers))
        .bindNamedParameter(NumPartitions.class, Integer.toString(numPartitions))
        .bindNamedParameter(EndpointId.class, contextId)
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
                .set(ServiceConfiguration.SERVICES, DynamicPartitionedParameterServer.class)
                .build())
            .bindImplementation(PartitionedParameterServer.class, DynamicPartitionedParameterServer.class)
            .bindImplementation(PartitionedServerSideReplySender.class, PartitionedServerSideReplySenderImpl.class)
            .bindNamedParameter(EndpointId.class, contextId)
            .bindNamedParameter(PSMessageHandler.class, PartitionedServerSideMsgHandler.class)
            .bindImplementation(ServerResolver.class, DynamicServerResolver.class)
            .bindNamedParameter(NumServers.class, Integer.toString(numServers))
            .bindNamedParameter(NumPartitions.class, Integer.toString(numPartitions))
            .bindNamedParameter(ServerNumThreads.class, Integer.toString(serverNumThreads))
            .bindNamedParameter(ServerQueueSize.class, Integer.toString(queueSize))
            .bindImplementation(BlockResolver.class, HashBlockResolver.class)
            .build());
  }
}
