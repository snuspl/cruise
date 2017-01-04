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
package edu.snu.cay.services.em.driver;

import edu.snu.cay.services.em.common.parameters.*;
import edu.snu.cay.services.em.driver.impl.BlockManager;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.evaluator.api.MigrationExecutor;
import edu.snu.cay.services.em.evaluator.api.RemoteAccessibleMemoryStore;
import edu.snu.cay.services.em.evaluator.api.RemoteOpHandler;
import edu.snu.cay.services.em.evaluator.impl.DataFirstMigrationExecutor;
import edu.snu.cay.services.em.evaluator.impl.EMMsgHandler;
import edu.snu.cay.services.em.evaluator.impl.OwnershipFirstMigrationExecutor;
import edu.snu.cay.services.em.msg.EMMsgCodec;
import edu.snu.cay.services.em.ns.NetworkContextRegister;
import edu.snu.cay.services.em.ns.NetworkDriverRegister;
import edu.snu.cay.services.em.ns.parameters.EMCodec;
import edu.snu.cay.services.em.ns.parameters.EMIdentifier;
import edu.snu.cay.services.em.ns.parameters.EMMessageHandler;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.driver.parameters.DriverStartHandler;
import org.apache.reef.evaluator.context.parameters.ContextStartHandlers;
import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;

/**
 * Configuration class for setting evaluator configurations of EMService.
 */
@DriverSide
public final class EMConfiguration {

  private final NameServer nameServer;
  private final LocalAddressProvider localAddressProvider;
  private final String driverId;
  private final int numTotalBlocks;
  private final int numStoreThreads;
  private final boolean rangeSupport;
  private final boolean consistencyPreserved;
  private final BlockManager blockManager;
  private final String identifier;

  @Inject
  private EMConfiguration(final NameServer nameServer,
                          final LocalAddressProvider localAddressProvider,
                          @Parameter(DriverIdentifier.class) final String driverId,
                          @Parameter(NumTotalBlocks.class) final int numTotalBlocks,
                          @Parameter(NumStoreThreads.class) final int numStoreThreads,
                          @Parameter(RangeSupport.class) final boolean rangeSupport,
                          @Parameter(ConsistencyPreserved.class) final boolean consistencyPreserved,
                          @Parameter(EMIdentifier.class) final String identifier,
                          final BlockManager blockManager) {
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.driverId = driverId;
    this.numTotalBlocks = numTotalBlocks;
    this.numStoreThreads = numStoreThreads;
    this.rangeSupport = rangeSupport;
    this.consistencyPreserved = consistencyPreserved;
    this.blockManager = blockManager;
    this.identifier = identifier;
  }

  /**
   * Configuration for REEF driver when using Elastic Memory.
   * Binds NetworkConnectionService registration handlers and EMMsg codec/handler.
   *
   * @return configuration that should be submitted with a DriverConfiguration
   */
  public static Configuration getDriverConfiguration() {
    return getNetworkConfigurationBuilder()
        .bindSetEntry(DriverStartHandler.class, NetworkDriverRegister.RegisterDriverHandler.class)
        .bindNamedParameter(EMMessageHandler.class, edu.snu.cay.services.em.driver.impl.EMMsgHandler.class)
        .build();
  }

  /**
   * Configuration for REEF driver when using Elastic Memory.
   * Different from {@link EMConfiguration#getDriverConfiguration()},
   * this version does not bind the RegisterDriverHandler.
   * The {@link edu.snu.cay.services.em.ns.EMNetworkSetup#registerConnectionFactory(org.apache.reef.wake.Identifier)}
   * should be called explicitly in {@link DriverStartHandler}.
   *
   * Note that this is a workaround to create two instances in dolphin async version for both Workers and Servers.
   *
   * @return configuration that should be submitted with a DriverConfiguration
   */
  public static Configuration getDriverConfigurationWithoutRegisterDriver() {
    return getNetworkConfigurationBuilder()
        .bindNamedParameter(EMMessageHandler.class, edu.snu.cay.services.em.driver.impl.EMMsgHandler.class)
        .build();
  }

  /**
   * Configuration for REEF context with Elastic Memory.
   * Binds NetworkConnectionService registration handlers.
   *
   * @return configuration that should be merged with a ContextConfiguration to form a context
   */
  public Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextStartHandlers.class, NetworkContextRegister.RegisterContextHandler.class)
        .bindSetEntry(ContextStopHandlers.class, NetworkContextRegister.UnregisterContextHandler.class)
        .build();
  }

  /**
   * Configuration for REEF service with Elastic Memory.
   * Sets up EMMsg codec/handler and MemoryStore, both required for Elastic Memory.
   *
   * @param contextId Identifier of the context that the service will run on
   * @param numInitialEvals The number of Evaluators that are allocated initially.
   * @return service configuration that should be passed along with a ContextConfiguration
   */
  public Configuration getServiceConfiguration(final String contextId,
                                               final int numInitialEvals) {
    final Configuration nameClientConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NameResolverNameServerPort.class, Integer.toString(nameServer.getPort()))
        .bindNamedParameter(NameResolverNameServerAddr.class, localAddressProvider.getLocalAddress())
        .build();

    return Configurations.merge(getServiceConfigurationWithoutNameResolver(contextId, numInitialEvals), nameClientConf);
  }

  public Configuration getServiceConfigurationWithoutNameResolver(final String contextId,
                                                                  final int numInitialEvals) {
    if (numTotalBlocks < numInitialEvals) {
      throw new RuntimeException("NumTotalBlocks should be greater than or equal to the number of NumInitialEvals.");
    }

    // implementations for MemoryStore and MsgHandler class differ regarding to range support
    // MemoryStore specialized to single-key operations is better in throughput and latency
    final Class memoryStoreClass = rangeSupport ?
        edu.snu.cay.services.em.evaluator.impl.rangekey.MemoryStoreImpl.class :
        edu.snu.cay.services.em.evaluator.impl.singlekey.MemoryStoreImpl.class;

    final Class remoteOpHandlerClass = rangeSupport ?
        edu.snu.cay.services.em.evaluator.impl.rangekey.RemoteOpHandlerImpl.class :
        edu.snu.cay.services.em.evaluator.impl.singlekey.RemoteOpHandlerImpl.class;

    final Class migrationExecutorClass = consistencyPreserved ?
        OwnershipFirstMigrationExecutor.class :
        DataFirstMigrationExecutor.class;

    final Class evalMsgHandlerClass = EMMsgHandler.class;

    final Configuration networkConf = getNetworkConfigurationBuilder()
        .bindNamedParameter(EMMessageHandler.class, evalMsgHandlerClass)
        .build();

    final Configuration serviceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, memoryStoreClass)
        .build();

    final int memoryStoreId = blockManager.registerEvaluator(contextId, numInitialEvals);

    final Configuration otherConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MemoryStore.class, memoryStoreClass)
        .bindImplementation(RemoteAccessibleMemoryStore.class, memoryStoreClass)
        .bindImplementation(RemoteOpHandler.class, remoteOpHandlerClass)
        .bindImplementation(MigrationExecutor.class, migrationExecutorClass)
        .bindNamedParameter(DriverIdentifier.class, driverId)
        .bindNamedParameter(MemoryStoreId.class, Integer.toString(memoryStoreId))
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(numTotalBlocks))
        .bindNamedParameter(NumStoreThreads.class, Integer.toString(numStoreThreads))
        .bindNamedParameter(NumInitialEvals.class, Integer.toString(numInitialEvals))
        .bindNamedParameter(KeyCodecName.class, SerializableCodec.class) // TODO #441: Make it configurable later.
        .bindNamedParameter(EMIdentifier.class, identifier)
        .build();

    return Configurations.merge(networkConf, serviceConf, otherConf);
  }

  private static JavaConfigurationBuilder getNetworkConfigurationBuilder() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(EMCodec.class, EMMsgCodec.class);
  }
}
