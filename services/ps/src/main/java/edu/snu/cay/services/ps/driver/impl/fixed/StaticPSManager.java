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
package edu.snu.cay.services.ps.driver.impl.fixed;

import edu.snu.cay.services.ps.driver.api.PSManager;
import edu.snu.cay.services.ps.ns.EndpointId;
import edu.snu.cay.services.ps.ns.PSMessageHandler;
import edu.snu.cay.services.ps.common.parameters.NumServers;
import edu.snu.cay.services.ps.common.parameters.NumPartitions;
import edu.snu.cay.services.ps.server.api.ParameterServer;
import edu.snu.cay.services.ps.server.api.ServerSideMsgSender;
import edu.snu.cay.services.ps.server.impl.fixed.StaticParameterServer;
import edu.snu.cay.services.ps.server.impl.ServerSideMsgHandler;
import edu.snu.cay.services.ps.server.impl.ServerSideMsgSenderImpl;
import edu.snu.cay.services.ps.server.parameters.ServerMetricsWindowMs;
import edu.snu.cay.services.ps.server.parameters.ServerNumThreads;
import edu.snu.cay.services.ps.server.parameters.ServerQueueSize;
import edu.snu.cay.services.ps.server.parameters.ServerLogPeriod;
import edu.snu.cay.services.ps.worker.api.WorkerHandler;
import edu.snu.cay.services.ps.worker.api.WorkerClock;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.services.ps.common.resolver.ServerResolver;
import edu.snu.cay.services.ps.common.resolver.StaticServerResolver;
import edu.snu.cay.services.ps.worker.impl.AsyncParameterWorker;
import edu.snu.cay.services.ps.worker.impl.NullWorkerClock;
import edu.snu.cay.services.ps.worker.impl.SSPWorkerClock;
import edu.snu.cay.services.ps.worker.impl.SSPParameterWorker;
import edu.snu.cay.services.ps.worker.parameters.*;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Driver-side manager class for the Static Parameter Server.
 * Partitions are based on the hash of the key.
 * Several servers threads are spawned (for each server) to handle disjoint sets of partitions.
 * Each server thread has its own queue and kvStore.
 * Moreover, the partition distribution across servers does not change at all.
 *
 * This manager does NOT handle server or worker faults.
 */
@DriverSide
public final class StaticPSManager implements PSManager {
  private final int numServers;
  private final int numPartitions;
  private final int workerNumThreads;
  private final int serverNumThreads;
  private final int workerQueueSize;
  private final int serverQueueSize;
  private final long workerExpireTimeout;
  private final long pullRetryTimeoutMs;
  private final int maxPendingPullsPerThread;
  private final int workerKeyCacheSize;
  private final long workerLogPeriod;
  private final long serverLogPeriod;
  private final long serverMetricsWindowMs;
  private final boolean isSSPModel;
  private final int stalenessBound;

  @Inject
  private StaticPSManager(@Parameter(NumServers.class) final int numServers,
                          @Parameter(NumPartitions.class) final int numPartitions,
                          @Parameter(ParameterWorkerNumThreads.class) final int workerNumThrs,
                          @Parameter(ServerNumThreads.class) final int serverNumThrs,
                          @Parameter(WorkerQueueSize.class) final int workerQueueSize,
                          @Parameter(ServerQueueSize.class) final int serverQueueSize,
                          @Parameter(WorkerExpireTimeout.class) final long workerExpireTimeout,
                          @Parameter(PullRetryTimeoutMs.class) final long pullRetryTimeoutMs,
                          @Parameter(MaxPendingPullsPerThread.class) final int maxPendingPullsPerThread,
                          @Parameter(WorkerKeyCacheSize.class) final int workerKeyCacheSize,
                          @Parameter(ServerMetricsWindowMs.class) final long serverMetricsWindowMs,
                          @Parameter(ServerLogPeriod.class) final long serverLogPeriod,
                          @Parameter(WorkerLogPeriod.class) final long workerLogPeriod,
                          @Parameter(StalenessBound.class) final int stalenessBound) {
    this.numServers = numServers;
    this.numPartitions = numPartitions;
    this.workerNumThreads = workerNumThrs;
    this.serverNumThreads = serverNumThrs;
    this.workerQueueSize = workerQueueSize;
    this.serverQueueSize = serverQueueSize;
    this.workerExpireTimeout = workerExpireTimeout;
    this.pullRetryTimeoutMs = pullRetryTimeoutMs;
    this.maxPendingPullsPerThread = maxPendingPullsPerThread;
    this.workerKeyCacheSize = workerKeyCacheSize;
    this.workerLogPeriod = workerLogPeriod;
    this.serverLogPeriod = serverLogPeriod;
    this.serverMetricsWindowMs = serverMetricsWindowMs;
    this.isSSPModel = stalenessBound >= 0;
    this.stalenessBound = stalenessBound;
  }

  /**
   * Returns worker-side service configuration.
   * Sets {@link AsyncParameterWorker} as the {@link ParameterWorker} class.
   */
  @Override
  public Configuration getWorkerServiceConfiguration(final String contextId) {
    return Tang.Factory.getTang()
        .newConfigurationBuilder(ServiceConfiguration.CONF
            .set(ServiceConfiguration.SERVICES,
                isSSPModel ? SSPParameterWorker.class : AsyncParameterWorker.class)
            .build())
        .bindImplementation(ParameterWorker.class,
            isSSPModel ? SSPParameterWorker.class : AsyncParameterWorker.class)
        .bindImplementation(WorkerClock.class,
            isSSPModel ? SSPWorkerClock.class : NullWorkerClock.class)
        .bindImplementation(WorkerHandler.class,
            isSSPModel ? SSPParameterWorker.class : AsyncParameterWorker.class)
        .bindImplementation(ServerResolver.class, StaticServerResolver.class)
        .bindNamedParameter(NumServers.class, Integer.toString(numServers))
        .bindNamedParameter(NumPartitions.class, Integer.toString(numPartitions))
        .bindNamedParameter(EndpointId.class, contextId)
        .bindNamedParameter(ParameterWorkerNumThreads.class, Integer.toString(workerNumThreads))
        .bindNamedParameter(WorkerQueueSize.class, Integer.toString(workerQueueSize))
        .bindNamedParameter(WorkerExpireTimeout.class, Long.toString(workerExpireTimeout))
        .bindNamedParameter(PullRetryTimeoutMs.class, Long.toString(pullRetryTimeoutMs))
        .bindNamedParameter(MaxPendingPullsPerThread.class, Integer.toString(maxPendingPullsPerThread))
        .bindNamedParameter(WorkerKeyCacheSize.class, Integer.toString(workerKeyCacheSize))
        .bindNamedParameter(WorkerLogPeriod.class, Long.toString(workerLogPeriod))
        .bindNamedParameter(StalenessBound.class, Integer.toString(stalenessBound))
        .build();
  }

  /**
   * Returns server-side service configuration.
   */
  @Override
  public Configuration getServerServiceConfiguration(final String contextId) {
    return Tang.Factory.getTang()
        .newConfigurationBuilder(ServiceConfiguration.CONF
            .set(ServiceConfiguration.SERVICES, StaticParameterServer.class)
            .build())
        .bindImplementation(ParameterServer.class, StaticParameterServer.class)
        .bindImplementation(ServerSideMsgSender.class, ServerSideMsgSenderImpl.class)
        .bindNamedParameter(EndpointId.class, contextId)
        .bindNamedParameter(PSMessageHandler.class, ServerSideMsgHandler.class)
        .bindImplementation(ServerResolver.class, StaticServerResolver.class)
        .bindNamedParameter(NumServers.class, Integer.toString(numServers))
        .bindNamedParameter(NumPartitions.class, Integer.toString(numPartitions))
        .bindNamedParameter(ServerNumThreads.class, Integer.toString(serverNumThreads))
        .bindNamedParameter(ServerQueueSize.class, Integer.toString(serverQueueSize))
        .bindNamedParameter(ServerLogPeriod.class, Long.toString(serverLogPeriod))
        .bindNamedParameter(ServerMetricsWindowMs.class, Long.toString(serverMetricsWindowMs))
        .build();
  }
}
