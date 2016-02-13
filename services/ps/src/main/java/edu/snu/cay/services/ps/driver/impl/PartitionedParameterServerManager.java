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
import edu.snu.cay.services.ps.server.partitioned.PartitionedParameterServer;
import edu.snu.cay.services.ps.server.partitioned.PartitionedServerSideMsgHandler;
import edu.snu.cay.services.ps.server.partitioned.PartitionedServerSideReplySender;
import edu.snu.cay.services.ps.server.partitioned.PartitionedServerSideReplySenderImpl;
import edu.snu.cay.services.ps.server.partitioned.parameters.ServerNumPartitions;
import edu.snu.cay.services.ps.server.partitioned.parameters.ServerQueueSize;
import edu.snu.cay.services.ps.worker.AsyncWorkerHandler;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.services.ps.worker.partitioned.ContextStopHandler;
import edu.snu.cay.services.ps.worker.partitioned.PartitionedParameterWorker;
import edu.snu.cay.services.ps.worker.partitioned.PartitionedWorkerHandler;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manager class for a Partitioned Parameter Server, that supports atomic,
 * in-order processing of push and pull operations, running on a single Evaluator.
 * Partitions are based on the hash of the key.
 * Each partition consists of a queue, kvStore, and thread.
 *
 * This manager does NOT handle server or worker faults.
 */
@DriverSide
public final class PartitionedParameterServerManager implements ParameterServerManager {
  private static final String SERVER_ID = "PARTITIONED_SERVER_ID";
  private static final String WORKER_ID_PREFIX = "PARTITIONED_WORKER_ID_";

  private final int numPartitions;
  private final int queueSize;
  private final AtomicInteger numWorkers;

  @Inject
  private PartitionedParameterServerManager(@Parameter(ServerNumPartitions.class) final int numPartitions,
                                            @Parameter(ServerQueueSize.class) final int queueSize) {
    this.numPartitions = numPartitions;
    this.queueSize = queueSize;
    this.numWorkers = new AtomicInteger(0);
  }

  /**
   * Returns worker-side service configuration.
   * Sets {@link PartitionedParameterWorker} as the {@link ParameterWorker} class.
   */
  @Override
  public Configuration getWorkerServiceConfiguration() {
    final int workerIndex = numWorkers.getAndIncrement();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(ServiceConfiguration.CONF
            .set(ServiceConfiguration.SERVICES, PartitionedParameterWorker.class)
            .set(ServiceConfiguration.ON_CONTEXT_STOP, ContextStopHandler.class)
            .build())
        .bindImplementation(ParameterWorker.class, PartitionedParameterWorker.class)
        .bindImplementation(AsyncWorkerHandler.class, PartitionedWorkerHandler.class)
        .bindNamedParameter(ServerId.class, SERVER_ID)
        .bindNamedParameter(EndpointId.class, WORKER_ID_PREFIX + workerIndex)
        .build();
  }

  /**
   * Returns server-side service configuration.
   */
  @Override
  public Configuration getServerServiceConfiguration() {
    return Tang.Factory.getTang()
        .newConfigurationBuilder(ServiceConfiguration.CONF
            .set(ServiceConfiguration.SERVICES, PartitionedParameterServer.class)
            .build())
        .bindNamedParameter(EndpointId.class, SERVER_ID)
        .bindNamedParameter(PSMessageHandler.class, PartitionedServerSideMsgHandler.class)
        .bindNamedParameter(ServerNumPartitions.class, Integer.toString(numPartitions))
        .bindNamedParameter(ServerQueueSize.class, Integer.toString(queueSize))
        .bindImplementation(PartitionedServerSideReplySender.class, PartitionedServerSideReplySenderImpl.class)
        .build();
  }
}
