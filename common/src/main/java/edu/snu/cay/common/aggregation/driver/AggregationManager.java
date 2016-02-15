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
package edu.snu.cay.common.aggregation.driver;

import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import edu.snu.cay.common.aggregation.params.SerializedAggregationSlavesConf;
import edu.snu.cay.common.aggregation.ns.MasterId;
import edu.snu.cay.common.aggregation.ns.NetworkContextRegister;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.evaluator.context.parameters.ContextStartHandlers;
import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;
import java.io.IOException;

/**
 * Manager for Aggregation Service.
 * Provides methods for getting context and service configurations.
 */
@DriverSide
public final class AggregationManager {

  private final NameServer nameServer;
  private final LocalAddressProvider localAddressProvider;
  private final Configuration slaveConf;
  private final String driverId;

  /**
   * Constructor for the manager of Aggregation Service.
   * This class is instantiated by TANG.
   *
   * @param nameServer a NameServer for NCS, which provides NameServer port
   * @param localAddressProvider a LocalAddressProvider for NCS, which provides NameServer address
   * @param configurationSerializer used for deserializing slave configuration
   * @param serializedSlaveConf serialized slave configuration which should not be instantiated in driver
   * @param driverId driver identifier
   * @throws IOException if there is a problem in deserializing slave configuration
   */
  @Inject
  private AggregationManager(final NameServer nameServer,
                             final LocalAddressProvider localAddressProvider,
                             final ConfigurationSerializer configurationSerializer,
                             @Parameter(SerializedAggregationSlavesConf.class) final String serializedSlaveConf,
                             @Parameter(DriverIdentifier.class) final String driverId) throws IOException {
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.slaveConf = configurationSerializer.fromString(serializedSlaveConf);
    this.driverId = driverId;
  }

  /**
   * Binds NetworkConnectionService registration handlers.
   * @return configuration to which a NetworkConnectionService registration handlers are added
   */
  public Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextStartHandlers.class, NetworkContextRegister.RegisterContextHandler.class)
        .bindSetEntry(ContextStopHandlers.class, NetworkContextRegister.UnregisterContextHandler.class)
        .build();
  }

  /**
   * Return the service configuration for the Aggregation Service.
   * @return service configuration for the Aggregation Service
   */
  public Configuration getServiceConfiguration() {
    final Configuration nameClientConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NameResolverNameServerPort.class, Integer.toString(nameServer.getPort()))
        .bindNamedParameter(NameResolverNameServerAddr.class, localAddressProvider.getLocalAddress())
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();

    return Configurations.merge(getServiceConfigurationWithoutNameResolver(), nameClientConf);
  }

  /**
   * Return the service configuration for the Aggregation Service without NameResolver.
   * @return service configuration for the Aggregation Service without NameResolver
   */
  public Configuration getServiceConfigurationWithoutNameResolver() {
    return Tang.Factory.getTang()
        .newConfigurationBuilder(ServiceConfiguration.CONF
                .set(ServiceConfiguration.SERVICES, AggregationSlave.class)
                .build(),
            slaveConf)
        .bindNamedParameter(MasterId.class, driverId)
        .build();
  }
}
