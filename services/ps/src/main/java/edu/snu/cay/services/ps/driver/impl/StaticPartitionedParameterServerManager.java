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

import edu.snu.cay.services.ps.driver.api.ParameterServerManager;
import edu.snu.cay.services.ps.ns.EndpointId;
import edu.snu.cay.services.ps.ns.PSMessageHandler;
import edu.snu.cay.services.ps.server.partitioned.*;
import edu.snu.cay.services.ps.common.partitioned.parameters.NumServers;
import edu.snu.cay.services.ps.common.partitioned.parameters.NumPartitions;
import edu.snu.cay.services.ps.server.partitioned.parameters.ServerNumThreads;
import edu.snu.cay.services.ps.server.partitioned.parameters.ServerQueueSize;
import edu.snu.cay.services.ps.worker.AsyncWorkerHandler;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.services.ps.worker.partitioned.ContextStopHandler;
import edu.snu.cay.services.ps.worker.partitioned.PartitionedParameterWorker;
import edu.snu.cay.services.ps.worker.partitioned.PartitionedWorkerHandler;
import edu.snu.cay.services.ps.common.partitioned.resolver.ServerResolver;
import edu.snu.cay.services.ps.common.partitioned.resolver.StaticServerResolver;
import edu.snu.cay.services.ps.worker.partitioned.parameters.ParameterWorkerNumThreads;
import edu.snu.cay.services.ps.worker.partitioned.parameters.WorkerExpireTimeout;
import edu.snu.cay.services.ps.worker.partitioned.parameters.WorkerKeyCacheSize;
import edu.snu.cay.services.ps.worker.partitioned.parameters.WorkerQueueSize;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;

import static edu.snu.cay.services.ps.common.Constants.SERVER_ID_PREFIX;
import static edu.snu.cay.services.ps.common.Constants.WORKER_ID_PREFIX;

/**
 * Manager class for a Partitioned Parameter Server, that supports atomic,
 * in-order processing of push and pull operations, running on a single Evaluator.
 * Partitions are based on the hash of the key.
 * Several servers threads are spawned (for each server) to handle disjoint sets of partitions.
 * Each server thread has its own queue and kvStore.
 *
 * This manager does NOT handle server or worker faults.
 */
@DriverSide
public final class StaticPartitionedParameterServerManager implements ParameterServerManager {
  private final int numServers;
  private final int numPartitions;
  private final int workerNumThreads;
  private final int serverNumThreads;
  private final int workerQueueSize;
  private final int serverQueueSize;
  private final long workerExpireTimeout;
  private final int workerKeyCacheSize;
  private final AtomicInteger workerCount;
  private final AtomicInteger serverCount;

  @Inject
  private StaticPartitionedParameterServerManager(@Parameter(NumServers.class) final int numServers,
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
    this.workerCount = new AtomicInteger(0);
    this.serverCount = new AtomicInteger(0);
  }

  /**
   * Returns worker-side service configuration.
   * Sets {@link PartitionedParameterWorker} as the {@link ParameterWorker} class.
   */
  @Override
  public Configuration getWorkerServiceConfiguration() {
    final int workerIndex = workerCount.getAndIncrement();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(ServiceConfiguration.CONF
            .set(ServiceConfiguration.SERVICES, PartitionedParameterWorker.class)
            .set(ServiceConfiguration.ON_CONTEXT_STOP, ContextStopHandler.class)
            .build())
        .bindImplementation(ParameterWorker.class, PartitionedParameterWorker.class)
        .bindImplementation(AsyncWorkerHandler.class, PartitionedWorkerHandler.class)
        .bindImplementation(ServerResolver.class, StaticServerResolver.class)
        .bindNamedParameter(NumServers.class, Integer.toString(numServers))
        .bindNamedParameter(NumPartitions.class, Integer.toString(numPartitions))
        .bindNamedParameter(EndpointId.class, WORKER_ID_PREFIX + workerIndex)
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
  public Configuration getServerServiceConfiguration() {
    final int serverIndex = serverCount.getAndIncrement();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(ServiceConfiguration.CONF
            .set(ServiceConfiguration.SERVICES, StaticPartitionedParameterServer.class)
            .build())
        .bindImplementation(PartitionedParameterServer.class, StaticPartitionedParameterServer.class)
        .bindImplementation(PartitionedServerSideReplySender.class, PartitionedServerSideReplySenderImpl.class)
        .bindNamedParameter(EndpointId.class, SERVER_ID_PREFIX + serverIndex)
        .bindNamedParameter(PSMessageHandler.class, PartitionedServerSideMsgHandler.class)
        .bindImplementation(ServerResolver.class, StaticServerResolver.class)
        .bindNamedParameter(NumServers.class, Integer.toString(numServers))
        .bindNamedParameter(NumPartitions.class, Integer.toString(numPartitions))
        .bindNamedParameter(ServerNumThreads.class, Integer.toString(serverNumThreads))
        .bindNamedParameter(ServerQueueSize.class, Integer.toString(serverQueueSize))
        .build();
  }
}
