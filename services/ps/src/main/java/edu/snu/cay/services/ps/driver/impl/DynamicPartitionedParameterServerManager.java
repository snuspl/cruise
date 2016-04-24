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

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.driver.AggregationMaster;
import edu.snu.cay.services.em.common.parameters.NumStoreThreads;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.driver.ElasticMemoryConfiguration;
import edu.snu.cay.services.em.driver.impl.PartitionManager;
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
import edu.snu.cay.services.ps.worker.partitioned.ContextStopHandler;
import edu.snu.cay.services.ps.worker.partitioned.DynamicServerResolver;
import edu.snu.cay.services.ps.worker.partitioned.PartitionedParameterWorker;
import edu.snu.cay.services.ps.worker.partitioned.PartitionedWorkerHandler;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

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
@Unit
@DriverSide
public final class DynamicPartitionedParameterServerManager implements ParameterServerManager {
  public static final String AGGREGATION_CLIENT_NAME = DynamicPartitionedParameterServerManager.class.getName();

  private final int numServers;
  private final int numPartitions;
  private final int serverNumThreads;
  private final int queueSize;
  private final AtomicInteger workerCount;
  private final AtomicInteger serverCount;
  private final AggregationMaster aggregationMaster;
  private final ElasticMemoryConfiguration emConf;

  @Inject
  private DynamicPartitionedParameterServerManager(@Parameter(NumServers.class)final int numServers,
                                                   @Parameter(NumPartitions.class) final int numPartitions,
                                                   @Parameter(ServerNumThreads.class) final int serverNumThreads,
                                                   @Parameter(ServerQueueSize.class) final int queueSize,
                                                   final AggregationMaster aggregationMaster,
                                                   final NameServer nameServer,
                                                   final LocalAddressProvider localAddressProvider,
                                                   final IdentifierFactory identifierFactory,
                                                   @Parameter(NameResolverNameServerAddr.class)
                                                     final String nameServerAddr,
                                                   @Parameter(NameResolverNameServerPort.class)
                                                     final int nameServerPort,
                                                   @Parameter(DriverIdentifier.class)final String driverId,
                                                   @Parameter(NumTotalBlocks.class) final int numTotalBlocks,
                                                   @Parameter(NumStoreThreads.class) final int numStoreThreads)
      throws InjectionException {
    this.numServers = numServers;
    this.numPartitions = numPartitions;
    this.serverNumThreads = serverNumThreads;
    this.queueSize = queueSize;
    this.workerCount = new AtomicInteger(0);
    this.serverCount = new AtomicInteger(0);
    this.aggregationMaster = aggregationMaster;

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(NameServer.class, nameServer);
    injector.bindVolatileInstance(IdentifierFactory.class, identifierFactory);
    injector.bindVolatileInstance(LocalAddressProvider.class, localAddressProvider);
    injector.bindVolatileParameter(NameResolverNameServerAddr.class, nameServerAddr);
    injector.bindVolatileParameter(NameResolverNameServerPort.class, nameServerPort);
    injector.bindVolatileParameter(DriverIdentifier.class, driverId);
    injector.bindVolatileParameter(NumTotalBlocks.class, numTotalBlocks);
    injector.bindVolatileParameter(NumStoreThreads.class, numStoreThreads);
    final PartitionManager partitionManager = injector.forkInjector().getInstance(PartitionManager.class);
    injector.bindVolatileInstance(PartitionManager.class, partitionManager);
    this.emConf = injector.getInstance(ElasticMemoryConfiguration.class);
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
        .bindImplementation(ServerResolver.class, DynamicServerResolver.class)
        .bindNamedParameter(NumServers.class, Integer.toString(numServers))
        .bindNamedParameter(NumPartitions.class, Integer.toString(numPartitions))
        .bindNamedParameter(EndpointId.class, WORKER_ID_PREFIX + workerIndex)
        .build();
  }

  /**
   * Returns server-side service configuration.
   */
  @Override
  public Configuration getServerServiceConfiguration() {
    final int serverIndex = serverCount.getAndIncrement();

    return Configurations.merge(
        emConf.getServiceConfigurationWithoutNameResolver(SERVER_ID_PREFIX + serverIndex, numServers),
        Tang.Factory.getTang().newConfigurationBuilder(
            ServiceConfiguration.CONF
                .set(ServiceConfiguration.SERVICES, DynamicPartitionedParameterServer.class)
                .build())
            .bindImplementation(PartitionedServerSideReplySender.class, PartitionedServerSideReplySenderImpl.class)
            .bindNamedParameter(EndpointId.class, SERVER_ID_PREFIX + serverIndex)
            .bindNamedParameter(PSMessageHandler.class, PartitionedServerSideMsgHandler.class)
            .bindImplementation(ServerResolver.class, DynamicServerResolver.class)
            .bindNamedParameter(NumServers.class, Integer.toString(numServers))
            .bindNamedParameter(NumPartitions.class, Integer.toString(numPartitions))
            .bindNamedParameter(ServerNumThreads.class, Integer.toString(serverNumThreads))
            .bindNamedParameter(ServerQueueSize.class, Integer.toString(queueSize))
            .build());
  }

  /**
   * Handles the message from ParameterWorkers for synchronizing ownership table.
   * Sends the global routing table.
   */
  public class MessageHandler implements EventHandler<AggregationMessage> {
    @Override
    public void onNext(final AggregationMessage aggregationMessage) {
      Logger.getLogger(MessageHandler.class.getName()).log(Level.SEVERE, "Received message from {0}",
          aggregationMessage.getSourceId());
      aggregationMaster.send(AGGREGATION_CLIENT_NAME, aggregationMessage.getSourceId().toString(), new byte[0]);
    }
  }
}
