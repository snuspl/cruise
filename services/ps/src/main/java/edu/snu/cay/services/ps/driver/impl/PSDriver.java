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

import edu.snu.cay.services.ps.PSParameters.SerializedCodecConfiguration;
import edu.snu.cay.services.ps.PSParameters.SerializedUpdaterConfiguration;
import edu.snu.cay.services.ps.driver.api.PSManager;
import edu.snu.cay.services.ps.ns.NetworkContextRegister;
import edu.snu.cay.services.ps.ns.PSMessageHandler;
import edu.snu.cay.services.ps.worker.impl.WorkerSideMsgHandler;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.evaluator.context.parameters.ContextStartHandlers;
import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;
import java.io.IOException;

/**
 * Driver code for Parameter Server applications.
 * Should be injected into the user's Driver class.
 * Provides methods for getting context and service configurations.
 */
// TODO #478: Change the confusing name of PSDriver
@DriverSide
public final class PSDriver {

  /**
   * Manager object that represents the Parameter Server configuration to be used.
   */
  private final PSManager psManager;

  /**
   * Configuration for key/preValue/value {@link org.apache.reef.wake.remote.Codec}s.
   */
  private final Configuration codecConfiguration;

  /**
   * Configuration that specifies the {@link edu.snu.cay.services.ps.server.api.ParameterUpdater} class to use.
   */
  private final Configuration updaterConfiguration;

  /**
   * Server for providing remote identifiers to the Network Connection Service.
   */
  private final NameServer nameServer;

  /**
   * Object needed for detecting the local address of this container.
   */
  private final LocalAddressProvider localAddressProvider;

  @Inject
  private PSDriver(final PSManager psManager,
                   final ConfigurationSerializer configurationSerializer,
                   @Parameter(SerializedCodecConfiguration.class) final String serializedCodecConf,
                   @Parameter(SerializedUpdaterConfiguration.class) final String serializedUpdaterConf,
                   final NameServer nameServer,
                   final LocalAddressProvider localAddressProvider) throws IOException, BindException {
    this.psManager = psManager;
    this.codecConfiguration = configurationSerializer.fromString(serializedCodecConf);
    this.updaterConfiguration = configurationSerializer.fromString(serializedUpdaterConf);
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
  }

  /**
   * Configuration for REEF driver when using Parameter Server.
   *
   * A message handler in the Driver is created, which is used to send EM's routing table information to Workers.
   * The driver is required to register to the NCS service, by calling
   * {@link edu.snu.cay.services.ps.ns.PSNetworkSetup#registerConnectionFactory(org.apache.reef.wake.Identifier)}
   * explicitly in the Driver's {@code EventHandler<StartTime>}. Note that this registration must be handled seamless
   * to users, but for dolphin async to create separate EM instances for Workers and Servers,
   * this class must be injected by the one who creates the server-side EM instance.
   *
   * @return configuration that should be submitted with a DriverConfiguration
   */
  public static Configuration getDriverConfiguration() {
    // TODO #551: Move such static configuration methods to other classes.
    // Note that this configuration is actually only for DynamicPS and not StaticPS - check package
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(PSMessageHandler.class, DriverSideMsgHandler.class)
        .build();
  }

  /**
   * @return context configuration for an Evaluator that uses {@code ParameterWorker} or {@code ParameterServer}
   */
  public Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextStartHandlers.class, NetworkContextRegister.RegisterContextHandler.class)
        .bindSetEntry(ContextStopHandlers.class, NetworkContextRegister.UnregisterContextHandler.class)
        .build();
  }

  /**
   * @return {@link NameServer} related configuration to be used by services
   */
  private Configuration getNameResolverServiceConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NameResolverNameServerPort.class, Integer.toString(nameServer.getPort()))
        .bindNamedParameter(NameResolverNameServerAddr.class, localAddressProvider.getLocalAddress())
        .build();
  }

  /**
   * @return service configuration for an Evaluator that uses a {@code ParameterWorker}
   */
  public Configuration getWorkerServiceConfiguration(final String contextId) {
    return Tang.Factory.getTang()
        .newConfigurationBuilder(
            codecConfiguration,
            psManager.getWorkerServiceConfiguration(contextId),
            updaterConfiguration,
            getNameResolverServiceConfiguration())
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .bindNamedParameter(PSMessageHandler.class, WorkerSideMsgHandler.class)
        .build();
  }

  /**
   * @return service configuration for an Evaluator that uses a {@code ParameterServer}
   */
  public Configuration getServerServiceConfiguration(final String contextId) {
    return Tang.Factory.getTang()
        .newConfigurationBuilder(
            codecConfiguration,
            psManager.getServerServiceConfiguration(contextId),
            updaterConfiguration,
            getNameResolverServiceConfiguration())
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();
  }
}
