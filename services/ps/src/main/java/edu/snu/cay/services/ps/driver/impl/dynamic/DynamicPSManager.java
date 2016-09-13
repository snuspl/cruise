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
import edu.snu.cay.services.em.evaluator.api.EMUpdateFunction;
import edu.snu.cay.services.em.evaluator.impl.HashBlockResolver;
import edu.snu.cay.services.ps.common.parameters.NumPartitions;
import edu.snu.cay.services.ps.common.parameters.NumServers;
import edu.snu.cay.services.ps.common.parameters.PSTraceProbability;
import edu.snu.cay.services.ps.common.resolver.DynamicServerResolver;
import edu.snu.cay.services.ps.common.resolver.ServerResolver;
import edu.snu.cay.services.ps.driver.api.PSManager;
import edu.snu.cay.services.ps.ns.EndpointId;
import edu.snu.cay.services.ps.ns.PSMessageHandler;
import edu.snu.cay.services.ps.server.api.ParameterServer;
import edu.snu.cay.services.ps.server.api.ServerSideReplySender;
import edu.snu.cay.services.ps.server.impl.ServerSideMsgHandler;
import edu.snu.cay.services.ps.server.impl.ServerSideReplySenderImpl;
import edu.snu.cay.services.ps.server.impl.dynamic.DynamicParameterServer;
import edu.snu.cay.services.ps.server.impl.dynamic.EMUpdateFunctionForPS;
import edu.snu.cay.services.ps.server.parameters.ServerLogPeriod;
import edu.snu.cay.services.ps.server.parameters.ServerMetricsWindowMs;
import edu.snu.cay.services.ps.server.parameters.ServerNumThreads;
import edu.snu.cay.services.ps.server.parameters.ServerQueueSize;
import edu.snu.cay.services.ps.worker.api.WorkerHandler;
import edu.snu.cay.services.ps.worker.api.WorkerClock;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.services.ps.worker.impl.AsyncParameterWorker;
import edu.snu.cay.services.ps.worker.impl.AsyncWorkerClock;
import edu.snu.cay.services.ps.worker.impl.SSPWorkerClock;
import edu.snu.cay.services.ps.worker.impl.SSPParameterWorker;
import edu.snu.cay.services.ps.worker.impl.dynamic.TaskStartHandler;
import edu.snu.cay.services.ps.worker.impl.dynamic.TaskStopHandler;
import edu.snu.cay.services.ps.worker.parameters.*;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Driver-side manager class for the Dynamic Parameter Server.
 * Partitions are logically determined by Elastic Memory, where each partition consists of
 * disjoint sets of blocks.
 * Each server spawns multiple threads each of which has its individual queue to handle operations.
 * Moreover, the partition distribution across servers may change -
 * this is managed by {@link DynamicServerResolver}.
 *
 * This manager does NOT handle server or worker faults.
 */
@DriverSide
public final class DynamicPSManager implements PSManager {

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
  private final double traceProbability;

  @Inject
  private DynamicPSManager(@Parameter(NumServers.class)final int numServers,
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
                           @Parameter(StalenessBound.class) final int stalenessBound,
                           @Parameter(PSTraceProbability.class) final double traceProbability) {
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
    this.traceProbability = traceProbability;
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
            .set(ServiceConfiguration.ON_TASK_STARTED, TaskStartHandler.class)
            .set(ServiceConfiguration.ON_TASK_STOP, TaskStopHandler.class)
            .build())
        .bindImplementation(ParameterWorker.class,
            isSSPModel ? SSPParameterWorker.class : AsyncParameterWorker.class)
        .bindImplementation(WorkerClock.class,
            isSSPModel ? SSPWorkerClock.class : AsyncWorkerClock.class)
        .bindImplementation(WorkerHandler.class,
            isSSPModel ? SSPParameterWorker.class : AsyncParameterWorker.class)
        .bindImplementation(ServerResolver.class, DynamicServerResolver.class)
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
        .bindNamedParameter(PSTraceProbability.class, Double.toString(traceProbability))
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
            .bindNamedParameter(PSMessageHandler.class, ServerSideMsgHandler.class)
            .bindImplementation(ServerResolver.class, DynamicServerResolver.class)
            .bindImplementation(EMUpdateFunction.class, EMUpdateFunctionForPS.class)
            .bindNamedParameter(NumServers.class, Integer.toString(numServers))
            .bindNamedParameter(NumPartitions.class, Integer.toString(numPartitions))
            .bindNamedParameter(ServerNumThreads.class, Integer.toString(serverNumThreads))
            .bindNamedParameter(ServerQueueSize.class, Integer.toString(serverQueueSize))
            .bindImplementation(BlockResolver.class, HashBlockResolver.class)
            .bindNamedParameter(ServerLogPeriod.class, Long.toString(serverLogPeriod))
            .bindNamedParameter(ServerMetricsWindowMs.class, Long.toString(serverMetricsWindowMs))
            .build());
  }
}
